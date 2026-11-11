"""Exact, cancellable storage measurement for a runtime SQLite database.

This is deliberately separate from :mod:`src.storage.db_table_sizes`. The
health route must answer in milliseconds, so its table breakdown is sampled on a
large database. This module is for an explicit background run where walking
every page is the point: ``dbstat`` reports the exact allocation of every table
and index, and ``job_sources`` is grouped by feed generation with exact row and
stored-text byte counts.

Some SQLite builds (notably the Windows Python wheel) omit ``dbstat``. In that
case table allocation is reported as unavailable rather than estimated, while
the ``job_sources`` generation totals still run exactly. A diagnostic that could
not measure allocation must not dress sampling up as exactness.
"""

from __future__ import annotations

import random
import sqlite3
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from src.storage_metrics import file_size

SCHEMA_VERSION = 1
PROGRESS_HANDLER_OPS = 2_000
PROGRESS_EMIT_INTERVAL_S = 0.5
QUICK_GENERATION_LIMIT = 50
QUICK_JOBS_TIMEOUT_S = 10.0
QUICK_TOTAL_BUDGET_S = 10.0
QUICK_ROW_BYTES_BUDGET_S = 3.0
QUICK_ROW_BYTES_SAMPLE_ROWS = 24
QUICK_ROW_BYTES_TABLES = ("job_sources", "jobs")
QUICK_ROW_BYTES_ROW_OVERHEAD = 8
QUICK_ROW_BYTES_SEED = 20261111
QUICK_CHECK_WARNING = (
    "Quick check reads current feed counts and page counters, counts jobs by "
    "generation within a bounded deadline, and samples stored row bytes for the "
    "current and older generations. Row-byte figures are estimates that exclude "
    "indexes; older generation counts and exact allocation remain in the exact "
    "diagnostic."
)

ProgressCallback = Callable[[dict[str, Any]], None]
CancelCallback = Callable[[], bool]


class StorageDiagnosticError(RuntimeError):
    """The diagnostic could not complete for a reason worth reporting."""


class StorageDiagnosticCancelled(StorageDiagnosticError):
    """The caller cancelled the diagnostic."""


def _pragma_int(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute(f"PRAGMA {name}").fetchone()
    return max(0, int(row[0] or 0)) if row is not None else 0


def _database_counters(conn: sqlite3.Connection) -> dict[str, int]:
    page_size = _pragma_int(conn, "page_size") or 4096
    page_count = _pragma_int(conn, "page_count")
    freelist_bytes = _pragma_int(conn, "freelist_count") * page_size
    return {
        "pageSize": page_size,
        "pageCount": page_count,
        "freelistBytes": freelist_bytes,
        "allocatedBytes": max(0, page_count * page_size - freelist_bytes),
    }


def _schema_tables(conn: sqlite3.Connection) -> tuple[list[str], dict[str, str]]:
    rows = conn.execute(
        """
        SELECT type, name, tbl_name
        FROM sqlite_schema
        WHERE type IN ('table', 'index')
        ORDER BY name
        """
    ).fetchall()
    table_names: list[str] = []
    index_parent: dict[str, str] = {}
    for row_type, name, tbl_name in rows:
        if not name:
            continue
        if row_type == "table":
            table_names.append(str(name))
        elif row_type == "index" and tbl_name:
            index_parent[str(name)] = str(tbl_name)
    return sorted(table_names), index_parent


def _quoted(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _job_sources_text_expression(conn: sqlite3.Connection) -> str:
    """Build an exact byte-count expression for every text-affinity column."""
    rows = conn.execute("PRAGMA table_info(job_sources)").fetchall()
    columns: list[str] = []
    for row in rows:
        if len(row) < 3 or not row[1]:
            continue
        declared = str(row[2] or "").upper()
        if not declared or any(token in declared for token in ("CHAR", "CLOB", "TEXT")):
            columns.append(_quoted(str(row[1])))
    if not columns:
        return "0"
    return " + ".join(f"length(CAST({column} AS BLOB))" for column in columns)


def _dbstat_tables(
    conn: sqlite3.Connection,
    table_names: list[str],
    index_parent: dict[str, str],
) -> tuple[list[dict[str, Any]], int] | None:
    """Return exact per-table allocation, or ``None`` if dbstat is unavailable."""
    try:
        rows = conn.execute(
            "SELECT name, SUM(pgsize), COUNT(*) FROM dbstat GROUP BY name"
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if "interrupted" in str(exc).lower():
            raise
        if "dbstat" not in str(exc).lower():
            raise
        return None

    known_tables = set(table_names)
    table_bytes = {name: 0 for name in table_names}
    index_bytes = {name: 0 for name in table_names}
    page_counts = {name: 0 for name in table_names}
    unassigned_bytes = 0
    for name, size, page_count in rows:
        key = str(name or "")
        size_bytes = max(0, int(size or 0))
        pages = max(0, int(page_count or 0))
        if key in known_tables:
            table_bytes[key] += size_bytes
            page_counts[key] += pages
            continue
        parent = index_parent.get(key)
        if parent in known_tables:
            index_bytes[parent] += size_bytes
            page_counts[parent] += pages
            continue
        unassigned_bytes += size_bytes

    result = [
        {
            "name": name,
            "tableBytes": table_bytes[name],
            "indexBytes": index_bytes[name],
            "bytes": table_bytes[name] + index_bytes[name],
            "pageCount": page_counts[name],
        }
        for name in table_names
    ]
    return result, unassigned_bytes


def _job_sources(conn: sqlite3.Connection) -> dict[str, Any]:
    text_expression = _job_sources_text_expression(conn)
    rows = conn.execute(
        f"""
        SELECT
            feed_generation,
            COUNT(*),
            COALESCE(SUM(length(CAST(payload_json AS BLOB))), 0),
            COALESCE(SUM({text_expression}), 0),
            MIN(updated_at),
            MAX(updated_at)
        FROM job_sources
        GROUP BY feed_generation
        ORDER BY COUNT(*) DESC, feed_generation
        """
    ).fetchall()
    feed_state = _feed_state(conn)
    current_generation = str(feed_state["currentGeneration"])

    by_generation: list[dict[str, Any]] = []
    for generation, row_count, payload_bytes, content_bytes, first_updated, last_updated in rows:
        generation_text = str(generation or "")
        by_generation.append(
            {
                "generation": generation_text,
                "rowCount": int(row_count or 0),
                "payloadBytes": int(payload_bytes or 0),
                "contentBytes": int(content_bytes or 0),
                "firstUpdatedAt": str(first_updated or ""),
                "lastUpdatedAt": str(last_updated or ""),
                "isCurrent": bool(generation_text and generation_text == current_generation),
                "isOrphan": bool(generation_text and generation_text != current_generation),
            }
        )
    by_generation.sort(
        key=lambda row: (not row["isCurrent"], -int(row["payloadBytes"]), row["generation"])
    )
    return {
        "table": "job_sources",
        "rowCount": sum(int(row["rowCount"]) for row in by_generation),
        "payloadBytes": sum(int(row["payloadBytes"]) for row in by_generation),
        "contentBytes": sum(int(row["contentBytes"]) for row in by_generation),
        "generationCount": len(by_generation),
        **feed_state,
        "byGeneration": by_generation,
    }


def _feed_state(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT current_generation, row_count, source_count, updated_at, published_at, run_id
        FROM job_feed_state
        WHERE id = 1
        """
    ).fetchone()
    if row is None:
        return {
            "currentGeneration": "",
            "currentRowCount": 0,
            "currentSourceCount": 0,
            "currentUpdatedAt": "",
            "currentPublishedAt": "",
            "currentRunId": "",
        }
    return {
        "currentGeneration": str(row[0] or ""),
        "currentRowCount": int(row[1] or 0),
        "currentSourceCount": int(row[2] or 0),
        "currentUpdatedAt": str(row[3] or ""),
        "currentPublishedAt": str(row[4] or ""),
        "currentRunId": str(row[5] or ""),
    }


def _generation_counts(
    conn: sqlite3.Connection,
    table: str,
    current_generation: str,
    *,
    limit: int = QUICK_GENERATION_LIMIT,
) -> dict[str, Any]:
    """Count rows by generation using the table's generation-leading index."""
    bounded_limit = max(1, int(limit))
    rows = conn.execute(
        f"""
        SELECT feed_generation, COUNT(*) AS row_count
        FROM {_quoted(table)}
        GROUP BY feed_generation
        ORDER BY row_count DESC, feed_generation ASC
        LIMIT ?
        """,
        (bounded_limit + 1,),
    ).fetchall()
    truncated = len(rows) > bounded_limit
    rows = rows[:bounded_limit]
    by_generation = [
        {
            "generation": str(generation or ""),
            "rowCount": int(cast(Any, row_count) or 0),
            "isCurrent": bool(generation and str(generation) == current_generation),
            "isOrphan": bool(generation and str(generation) != current_generation),
        }
        for generation, row_count in rows
    ]
    return {
        "table": table,
        "listedRowCount": sum(cast(int, row["rowCount"]) for row in by_generation),
        "generationCount": len(by_generation),
        "truncated": truncated,
        "byGeneration": by_generation,
    }


class _DiagnosticProgress:
    """Track progress and cancellation outside the measurement body."""

    def __init__(
        self,
        *,
        started: float,
        progress: ProgressCallback | None,
        cancel_requested: CancelCallback | None,
    ) -> None:
        self.started = started
        self.progress = progress
        self.cancel_requested = cancel_requested
        self.phase = "opening"
        self.operation_count = 0
        self.last_emit = 0.0

    def emit(self, *, force: bool = False) -> None:
        if self.progress is None:
            return
        now = time.monotonic()
        if not force and now - self.last_emit < PROGRESS_EMIT_INTERVAL_S:
            return
        self.last_emit = now
        self.progress(
            {
                "phase": self.phase,
                "operationCount": self.operation_count,
                "elapsedMs": int(round((now - self.started) * 1000)),
            }
        )

    def check_cancelled(self) -> None:
        if self.was_cancelled():
            raise StorageDiagnosticCancelled("storage diagnostic cancelled")

    def was_cancelled(self) -> bool:
        return self.cancel_requested is not None and self.cancel_requested()

    def set_phase(self, phase: str) -> None:
        self.phase = phase
        self.emit(force=True)

    def sqlite_progress(self) -> int:
        self.operation_count += PROGRESS_HANDLER_OPS
        if self.was_cancelled():
            return 1
        self.emit()
        return 0


def _current_generation_counts(
    table: str,
    generation: str,
    row_count: int,
) -> dict[str, Any]:
    """Build the exact current-generation section from job_feed_state."""
    current = str(generation or "")
    count = max(0, int(row_count or 0))
    by_generation: list[dict[str, Any]] = []
    if current:
        by_generation.append(
            {
                "generation": current,
                "rowCount": count,
                "isCurrent": True,
                "isOrphan": False,
            }
        )
    return {
        "table": table,
        "listedRowCount": count if current else 0,
        "generationCount": 1 if current else 0,
        "truncated": False,
        "scope": "current",
        "byGeneration": by_generation,
    }


def _stored_row_length_expression(conn: sqlite3.Connection, table: str) -> tuple[str, int]:
    """Return a SQL expression for stored row bytes and its column count."""
    rows = conn.execute(f"PRAGMA table_info({_quoted(table)})").fetchall()
    columns = [str(row[1]) for row in rows if len(row) > 1 and row[1]]
    if not columns:
        return "0", 0
    expression = " + ".join(
        f"COALESCE(length(CAST({_quoted(column)} AS BLOB)), 0)" for column in columns
    )
    return expression, len(columns)


def _row_bytes_current_sample(
    conn: sqlite3.Connection,
    *,
    table: str,
    expression: str,
    overhead: int,
    generation: str,
    row_count: int,
    deadline: float,
) -> tuple[int, int, int]:
    if time.monotonic() >= deadline:
        raise TimeoutError
    rows = conn.execute(
        f"""
        SELECT {expression}
        FROM {_quoted(table)}
        WHERE feed_generation = ?
        LIMIT ?
        """,
        (generation, QUICK_ROW_BYTES_SAMPLE_ROWS),
    ).fetchall()
    lengths = [int(row[0] or 0) for row in rows]
    if not lengths:
        return 0, 0, 0
    average = int(round(sum(lengths) / len(lengths))) + overhead
    return len(lengths), average, average * max(0, int(row_count))


def _row_bytes_total_sample(
    conn: sqlite3.Connection,
    *,
    table: str,
    expression: str,
    overhead: int,
    rng: random.Random,
    deadline: float,
) -> tuple[int, int, int | None, int | None, bool]:
    bounds = conn.execute(f"SELECT MIN(rowid), MAX(rowid) FROM {_quoted(table)}").fetchone()
    if bounds is None or bounds[0] is None or bounds[1] is None:
        return 0, 0, 0, 0, False
    if time.monotonic() >= deadline:
        raise TimeoutError
    low, high = int(bounds[0]), int(bounds[1])
    span = max(1, high - low + 1)
    hits = 0
    sampled_bytes = 0
    attempts = 0
    while attempts < QUICK_ROW_BYTES_SAMPLE_ROWS:
        if time.monotonic() >= deadline:
            break
        row = conn.execute(
            f"""
            SELECT {expression}
            FROM {_quoted(table)}
            WHERE rowid = ?
            """,
            (rng.randint(low, high),),
        ).fetchone()
        attempts += 1
        if row is None:
            continue
        hits += 1
        sampled_bytes += int(row[0] or 0) + overhead
    partial = attempts < QUICK_ROW_BYTES_SAMPLE_ROWS
    if not hits:
        return attempts, 0, None, None, partial
    total_rows = int(round(span * hits / max(1, attempts)))
    total_bytes = int(round((sampled_bytes / hits) * total_rows))
    return attempts, hits, total_rows, total_bytes, partial


def _finish_row_bytes_entry(entry: dict[str, Any], warnings: list[str]) -> None:
    total_bytes = entry.get("totalBytes")
    current_bytes = entry.get("currentBytes")
    if total_bytes is None or current_bytes is None:
        return
    total_bytes_int = max(0, int(total_bytes))
    current_bytes_int = max(0, int(current_bytes))
    if current_bytes_int > total_bytes_int and total_bytes_int > 0:
        warnings.append(
            f"{entry['table']} current-generation sample exceeded the table estimate; "
            "sharing is clamped."
        )
    entry["oldBytes"] = max(0, total_bytes_int - current_bytes_int)
    if total_bytes_int > 0:
        entry["currentShare"] = round(min(1.0, current_bytes_int / total_bytes_int), 4)


def _row_bytes_table_probe(
    conn: sqlite3.Connection,
    *,
    table: str,
    count: int,
    current_generation: str,
    rng: random.Random,
    deadline: float,
    progress: _DiagnosticProgress,
    warnings: list[str],
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "table": table,
        "status": "ok",
        "currentRows": count,
        "currentSampleRows": 0,
        "currentAvgRowBytes": None,
        "currentBytes": None,
        "estimatedTotalRows": None,
        "totalSampleRows": 0,
        "totalHits": 0,
        "totalBytes": None,
        "oldBytes": None,
        "currentShare": None,
        "error": "",
    }
    try:
        expression, column_count = _stored_row_length_expression(conn, table)
        overhead = QUICK_ROW_BYTES_ROW_OVERHEAD * max(1, column_count)
        (
            entry["currentSampleRows"],
            entry["currentAvgRowBytes"],
            entry["currentBytes"],
        ) = _row_bytes_current_sample(
            conn,
            table=table,
            expression=expression,
            overhead=overhead,
            generation=current_generation,
            row_count=count,
            deadline=deadline,
        )
        attempts, hits, total_rows, total_bytes, partial = _row_bytes_total_sample(
            conn,
            table=table,
            expression=expression,
            overhead=overhead,
            rng=rng,
            deadline=deadline,
        )
        entry["totalSampleRows"] = attempts
        entry["totalHits"] = hits
        entry["estimatedTotalRows"] = total_rows
        entry["totalBytes"] = total_bytes
        if hits == 0:
            entry["status"] = "unavailable"
            entry["error"] = "rowid sample found no live rows"
            warnings.append(f"{table} row-byte sample found no live rows.")
        elif partial:
            entry["status"] = "partial"
            warnings.append(
                f"{table} row-byte sample hit the {QUICK_ROW_BYTES_BUDGET_S:g}s budget."
            )
        _finish_row_bytes_entry(entry, warnings)
    except TimeoutError:
        entry["status"] = "partial"
        entry["error"] = f"row-byte sample exceeded {QUICK_ROW_BYTES_BUDGET_S:g}s"
        warnings.append(
            f"{table} row-byte sample exceeded the {QUICK_ROW_BYTES_BUDGET_S:g}s budget."
        )
    except sqlite3.OperationalError as exc:
        if progress.was_cancelled():
            raise StorageDiagnosticCancelled("storage quick check cancelled") from exc
        if "interrupt" in str(exc).lower():
            entry["status"] = "partial"
            entry["error"] = f"row-byte sample exceeded {QUICK_ROW_BYTES_BUDGET_S:g}s"
            warnings.append(
                f"{table} row-byte sample exceeded the {QUICK_ROW_BYTES_BUDGET_S:g}s budget."
            )
        else:
            entry["status"] = "unavailable"
            entry["error"] = str(exc)
            warnings.append(f"{table} row-byte sample unavailable: {exc}")
    return entry


def _row_bytes_classification(current_share: float | None) -> str:
    if current_share is None:
        return "inconclusive"
    if current_share <= 0.30:
        return "old-generations"
    if current_share >= 0.65:
        return "current-generation"
    return "mixed"


def _row_bytes_summary(classification: str, current_share: float | None) -> str:
    if classification == "old-generations":
        return f"Most sampled row bytes are in older generations ({current_share:.1%} current)."
    if classification == "current-generation":
        return (
            f"Most sampled row bytes are in the current generation ({current_share:.1%} current)."
        )
    if classification == "mixed":
        return (
            "Both current and older generations contribute sampled row bytes "
            f"({current_share:.1%} current)."
        )
    return "Row-byte totals could not be estimated from the bounded sample."


def _row_bytes_payload(
    tables: list[dict[str, Any]],
    warnings: list[str],
    *,
    started: float,
) -> dict[str, Any]:
    available = [
        row
        for row in tables
        if row.get("totalBytes") is not None and row.get("currentBytes") is not None
    ]
    if all(row.get("status") == "ok" for row in tables):
        status = "ok"
    elif available:
        status = "partial"
    else:
        status = "unavailable"
    current_bytes = sum(int(row.get("currentBytes") or 0) for row in available)
    total_bytes = sum(int(row.get("totalBytes") or 0) for row in available)
    old_bytes = sum(int(row.get("oldBytes") or 0) for row in available)
    if current_bytes > total_bytes and total_bytes > 0:
        warnings.append(
            "The current-generation sample exceeded the total row-byte estimate; "
            "sharing is clamped to the total."
        )
        current_bytes = total_bytes
        old_bytes = 0
    current_share = round(current_bytes / total_bytes, 4) if total_bytes > 0 else None
    old_share = round(1.0 - current_share, 4) if current_share is not None else None
    classification = _row_bytes_classification(current_share)
    summary = _row_bytes_summary(classification, current_share)
    if status != "ok":
        summary = f"Provisional estimate. {summary}"
    return {
        "status": status,
        "method": "bounded-rowid-sample",
        "sampleRows": QUICK_ROW_BYTES_SAMPLE_ROWS,
        "budgetMs": int(round(QUICK_ROW_BYTES_BUDGET_S * 1000)),
        "durationMs": int(round((time.monotonic() - started) * 1000)),
        "currentBytes": current_bytes,
        "oldBytes": old_bytes,
        "totalBytes": total_bytes,
        "currentShare": current_share,
        "oldShare": old_share,
        "classification": classification,
        "summary": summary,
        "tables": tables,
        "warnings": list(dict.fromkeys(warnings)),
    }


def _row_bytes_probe(
    conn: sqlite3.Connection,
    *,
    current_generation: str,
    source_count: int,
    job_count: int,
    progress: _DiagnosticProgress,
) -> dict[str, Any]:
    """Estimate stored row bytes in the current and older feed generations.

    The probe samples current-generation rows through their generation-leading
    index and samples the whole table through random rowids. Both samples are
    bounded by a deadline and return lengths, never row payloads. The total
    rowid sample is deliberately not a generation count: it estimates physical
    row bytes, which is the question the Quick check could not answer on a
    multi-hundred-gigabyte store.
    """
    started = time.monotonic()
    deadline = started + max(0.25, float(QUICK_ROW_BYTES_BUDGET_S))
    table_budget_s = max(0.25, (deadline - started) / max(1, len(QUICK_ROW_BYTES_TABLES)))
    counts = {
        "job_sources": max(0, int(source_count or 0)),
        "jobs": max(0, int(job_count or 0)),
    }
    rng = random.Random(QUICK_ROW_BYTES_SEED)
    tables: list[dict[str, Any]] = []
    warnings: list[str] = []

    def _probe_progress() -> int:
        if progress.was_cancelled():
            return 1
        progress.operation_count += PROGRESS_HANDLER_OPS
        progress.emit()
        return 1 if time.monotonic() >= deadline else 0

    conn.set_progress_handler(_probe_progress, PROGRESS_HANDLER_OPS)
    try:
        for table in QUICK_ROW_BYTES_TABLES:
            table_deadline = min(deadline, time.monotonic() + table_budget_s)
            tables.append(
                _row_bytes_table_probe(
                    conn,
                    table=table,
                    count=counts[table],
                    current_generation=current_generation,
                    rng=rng,
                    deadline=table_deadline,
                    progress=progress,
                    warnings=warnings,
                )
            )
    finally:
        conn.set_progress_handler(progress.sqlite_progress, PROGRESS_HANDLER_OPS)
    return _row_bytes_payload(tables, warnings, started=started)


def _bounded_generation_counts(
    conn: sqlite3.Connection,
    table: str,
    current_generation: str,
    *,
    timeout_s: float = QUICK_JOBS_TIMEOUT_S,
    progress: _DiagnosticProgress | None = None,
) -> tuple[dict[str, Any] | None, str]:
    """Count one generation-leading table, returning partial status on timeout."""
    deadline = time.monotonic() + max(0.1, float(timeout_s))

    def _bounded_progress() -> int:
        if progress is not None:
            if progress.was_cancelled():
                return 1
            progress.operation_count += PROGRESS_HANDLER_OPS
            progress.emit()
        return 1 if time.monotonic() >= deadline else 0

    conn.set_progress_handler(_bounded_progress, PROGRESS_HANDLER_OPS)
    try:
        return _generation_counts(conn, table, current_generation), ""
    except sqlite3.OperationalError as exc:
        if progress is not None and progress.was_cancelled():
            raise StorageDiagnosticCancelled("storage quick check cancelled") from exc
        if "interrupt" in str(exc).lower():
            return None, f"generation count exceeded {timeout_s:g} seconds"
        raise
    finally:
        conn.set_progress_handler(
            progress.sqlite_progress if progress is not None else None,
            PROGRESS_HANDLER_OPS,
        )


def _open_readonly(path: Path, progress: _DiagnosticProgress) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        conn.execute("PRAGMA query_only = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute("PRAGMA cache_size = -8192")
        conn.set_progress_handler(progress.sqlite_progress, PROGRESS_HANDLER_OPS)
    except BaseException:
        conn.close()
        raise
    return conn


def _table_sizes_payload(
    conn: sqlite3.Connection,
    table_names: list[str],
    index_parent: dict[str, str],
    counters: dict[str, int],
    *,
    database_bytes: int,
    started: float,
) -> tuple[dict[str, Any], list[dict[str, Any]], bool]:
    dbstat_result = _dbstat_tables(conn, table_names, index_parent)
    if dbstat_result is None:
        allocation_available = False
        table_rows: list[dict[str, Any]] = []
        unassigned_bytes = 0
    else:
        allocation_available = True
        table_rows, unassigned_bytes = dbstat_result
    for row in table_rows:
        row["share"] = round(int(row["bytes"]) / max(1, int(counters["allocatedBytes"])), 4)
    table_rows.sort(key=lambda row: int(row["bytes"]), reverse=True)
    table_total_bytes = sum(int(row["bytes"]) for row in table_rows)
    table_sizes: dict[str, Any] = {
        "status": "ok" if allocation_available else "unavailable",
        "method": "dbstat" if allocation_available else "unavailable",
        "estimated": False,
        "complete": allocation_available,
        "capturedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "durationMs": int(round((time.monotonic() - started) * 1000)),
        "databaseBytes": database_bytes,
        **counters,
        "tables": table_rows,
        "tableCount": len(table_rows),
        "tableTotalBytes": table_total_bytes,
        "otherBytes": max(0, int(counters["allocatedBytes"]) - table_total_bytes),
        "unassignedBytes": unassigned_bytes,
        "truncated": False,
        "error": ""
        if allocation_available
        else (
            "This SQLite build does not include dbstat, so exact table "
            "allocation is unavailable; job_sources generation totals are exact."
        ),
    }
    return table_sizes, table_rows, allocation_available


def _completed_payload(
    *,
    path: Path,
    started: float,
    database_bytes: int,
    wal_bytes: int,
    shm_bytes: int,
    table_sizes: dict[str, Any],
    table_rows: list[dict[str, Any]],
    allocation_available: bool,
    job_sources: dict[str, Any],
) -> dict[str, Any]:
    job_sources["tableBytes"] = next(
        (int(row["bytes"]) for row in table_rows if row["name"] == "job_sources"),
        None,
    )
    return {
        "schemaVersion": SCHEMA_VERSION,
        "mode": "exact",
        "status": "complete",
        "capturedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "durationMs": int(round((time.monotonic() - started) * 1000)),
        "databasePath": str(path),
        "databaseBytes": database_bytes,
        "walBytes": wal_bytes,
        "shmBytes": shm_bytes,
        "exactContent": True,
        "exactAllocation": allocation_available,
        "warnings": []
        if allocation_available
        else ["SQLite dbstat is unavailable; table allocation could not be measured exactly."],
        "tableSizes": table_sizes,
        "jobSources": job_sources,
    }


def measure_storage_quick_check(
    db_path: Path | str,
    *,
    progress: ProgressCallback | None = None,
    cancel_requested: CancelCallback | None = None,
) -> dict[str, Any]:
    """Return current feed counts and bounded job counts without dbstat."""
    started = time.monotonic()
    path = Path(db_path)
    tracker = _DiagnosticProgress(
        started=started, progress=progress, cancel_requested=cancel_requested
    )
    if not path.is_file():
        raise StorageDiagnosticError("database file not found")
    tracker.check_cancelled()
    tracker.set_phase("counters")
    database_bytes = file_size(path)
    wal_bytes = file_size(path.with_name(f"{path.name}-wal"))
    shm_bytes = file_size(path.with_name(f"{path.name}-shm"))
    conn: sqlite3.Connection | None = None
    try:
        conn = _open_readonly(path, tracker)
        counters = _database_counters(conn)
        tracker.set_phase("feed_state")
        feed_state = _feed_state(conn)
        tracker.check_cancelled()
        current_generation = str(feed_state["currentGeneration"])
        job_sources = _current_generation_counts(
            "job_sources",
            current_generation,
            int(feed_state["currentSourceCount"] or 0),
        )
        tracker.check_cancelled()
        tracker.set_phase("row_bytes")
        row_bytes_probe = _row_bytes_probe(
            conn,
            current_generation=current_generation,
            source_count=int(feed_state["currentSourceCount"] or 0),
            job_count=int(feed_state["currentRowCount"] or 0),
            progress=tracker,
        )
        tracker.check_cancelled()
        tracker.set_phase("jobs")
        remaining_s = max(0.1, QUICK_TOTAL_BUDGET_S - (time.monotonic() - started))
        jobs, jobs_error = _bounded_generation_counts(
            conn,
            "jobs",
            current_generation,
            timeout_s=remaining_s,
            progress=tracker,
        )
        warnings = [QUICK_CHECK_WARNING, *row_bytes_probe["warnings"]]
        if jobs is None:
            jobs = _current_generation_counts(
                "jobs",
                current_generation,
                int(feed_state["currentRowCount"] or 0),
            )
            jobs["status"] = "partial"
            jobs["error"] = jobs_error
            warnings.append(
                f"Jobs by generation did not finish: {jobs_error}. "
                "The current generation count is from job_feed_state."
            )
        tracker.check_cancelled()
        tracker.set_phase("finalizing")
        return {
            "schemaVersion": SCHEMA_VERSION,
            "mode": "quick",
            "status": "complete",
            "capturedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "durationMs": int(round((time.monotonic() - started) * 1000)),
            "databasePath": str(path),
            "databaseBytes": database_bytes,
            "walBytes": wal_bytes,
            "shmBytes": shm_bytes,
            "pageSize": counters["pageSize"],
            "pageCount": counters["pageCount"],
            "freelistBytes": counters["freelistBytes"],
            "allocatedBytes": counters["allocatedBytes"],
            "exactContent": not jobs_error,
            "partial": bool(jobs_error or row_bytes_probe["status"] != "ok"),
            "exactAllocation": False,
            "tableSizes": None,
            "jobSources": None,
            "feedState": feed_state,
            "jobSourcesByGeneration": job_sources,
            "jobsByGeneration": jobs,
            "rowBytesProbe": row_bytes_probe,
            "warnings": warnings,
        }
    except sqlite3.OperationalError as exc:
        if tracker.was_cancelled():
            raise StorageDiagnosticCancelled("storage quick check cancelled") from exc
        raise StorageDiagnosticError(str(exc)) from exc
    finally:
        if conn is not None:
            conn.close()
            tracker.emit(force=True)


def measure_storage_diagnostic(
    db_path: Path | str,
    *,
    progress: ProgressCallback | None = None,
    cancel_requested: CancelCallback | None = None,
) -> dict[str, Any]:
    """Measure exact table allocation and exact job-source generation totals.

    The function is read-only and synchronous; the bridge runs it in a daemon
    thread. ``cancel_requested`` is checked from SQLite's progress handler, so a
    long ``dbstat`` walk can be stopped without waiting for the whole scan.
    """
    started = time.monotonic()
    path = Path(db_path)
    tracker = _DiagnosticProgress(
        started=started, progress=progress, cancel_requested=cancel_requested
    )
    if not path.is_file():
        raise StorageDiagnosticError("database file not found")
    tracker.check_cancelled()
    tracker.set_phase("opening")
    database_bytes = file_size(path)
    wal_bytes = file_size(path.with_name(f"{path.name}-wal"))
    shm_bytes = file_size(path.with_name(f"{path.name}-shm"))
    conn: sqlite3.Connection | None = None
    try:
        conn = _open_readonly(path, tracker)
        counters = _database_counters(conn)
        table_names, index_parent = _schema_tables(conn)
        tracker.set_phase("tables")
        table_sizes, table_rows, allocation_available = _table_sizes_payload(
            conn,
            table_names,
            index_parent,
            counters,
            database_bytes=database_bytes,
            started=started,
        )
        tracker.check_cancelled()
        tracker.set_phase("job_sources")
        job_sources = _job_sources(conn)
        tracker.check_cancelled()
        tracker.set_phase("finalizing")
        return _completed_payload(
            path=path,
            started=started,
            database_bytes=database_bytes,
            wal_bytes=wal_bytes,
            shm_bytes=shm_bytes,
            table_sizes=table_sizes,
            table_rows=table_rows,
            allocation_available=allocation_available,
            job_sources=job_sources,
        )
    except sqlite3.OperationalError as exc:
        if tracker.was_cancelled():
            raise StorageDiagnosticCancelled("storage diagnostic cancelled") from exc
        raise StorageDiagnosticError(str(exc)) from exc
    finally:
        if conn is not None:
            conn.close()
            tracker.emit(force=True)
