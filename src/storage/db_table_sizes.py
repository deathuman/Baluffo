"""Bounded per-table space breakdown for the runtime SQLite database.

`/ops/storage-health` has to answer where the database space went without
becoming a second storage problem. A full `dbstat` traversal is exact but
unbounded: on the live runtime database it means walking hundreds of gigabytes,
so it is only used when the file is small. Everything larger is estimated from
uniformly sampled rowids:

* a table's rowid span and its live-row hit rate estimate the row count;
* the sampled rows' stored bytes estimate the average row size.

Sampling performs a bounded number of b-tree lookups and checks a deadline
between samples, so a multi-hundred-gigabyte store still answers in bounded
time. The payload always states the method and whether the numbers are
estimated or complete.
"""

from __future__ import annotations

import random
import sqlite3
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_SAMPLE_ROWS_PER_TABLE = 16
DEFAULT_MAX_TABLES = 12
DEFAULT_BUDGET_MS = 750
DEFAULT_EXACT_MAX_DATABASE_BYTES = 64 * 1024 * 1024
_SAMPLE_SEED = 20261109
_ROW_OVERHEAD_BYTES = 8


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _quoted(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _row_stored_bytes(row: Sequence[Any]) -> int:
    """Approximate stored bytes for one row, including overflow payload text."""
    total = _ROW_OVERHEAD_BYTES
    for value in row:
        if value is None:
            total += 1
        elif isinstance(value, bytes):
            total += len(value) + 3
        elif isinstance(value, str):
            total += len(value.encode("utf-8", "replace")) + 2
        elif isinstance(value, (int, float)):
            total += 8
        else:
            total += len(str(value)) + 2
    return total


def _table_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT name FROM sqlite_schema
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [str(row[0]) for row in rows if row and row[0]]


def _pragma_int(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute(f"PRAGMA {name}").fetchone()
    return max(0, int(row[0] or 0)) if row is not None else 0


def _measure_exact(conn: sqlite3.Connection, table_names: list[str]) -> dict[str, int] | None:
    """Exact per-table bytes from dbstat, or None when the build lacks dbstat."""
    try:
        rows = conn.execute("SELECT name, SUM(pgsize) FROM dbstat GROUP BY name").fetchall()
    except sqlite3.OperationalError:
        return None
    known = set(table_names)
    sizes = {name: 0 for name in table_names}
    for row in rows:
        key = str(row[0] or "")
        if key in known:
            sizes[key] += max(0, int(row[1] or 0))
    return sizes


def _estimate_table(
    conn: sqlite3.Connection,
    name: str,
    *,
    samples: int,
    rng: random.Random,
    deadline: float,
) -> tuple[dict[str, Any] | None, bool]:
    """Estimate one table; None means the table has no rowid (WITHOUT ROWID)."""
    table = _quoted(name)
    try:
        bounds = conn.execute(f"SELECT MIN(rowid), MAX(rowid) FROM {table}").fetchone()
    except sqlite3.OperationalError:
        return None, False
    if bounds is None or bounds[0] is None or bounds[1] is None:
        return {"name": name, "rowCount": 0, "bytes": 0, "sampledRows": 0}, False
    low, high = int(bounds[0]), int(bounds[1])
    span = max(1, high - low + 1)
    hits = 0
    sampled_bytes = 0
    attempted = 0
    timed_out = False
    for _ in range(samples):
        if time.monotonic() >= deadline:
            if attempted == 0:
                return None, True
            timed_out = True
            break
        row = conn.execute(
            f"SELECT * FROM {table} WHERE rowid = ?", (rng.randint(low, high),)
        ).fetchone()
        attempted += 1
        if row is None:
            continue
        hits += 1
        sampled_bytes += _row_stored_bytes(row)
    if hits == 0 or attempted == 0:
        return {"name": name, "rowCount": 0, "bytes": 0, "sampledRows": 0}, timed_out
    row_count = int(round(span * hits / attempted))
    return {
        "name": name,
        "rowCount": row_count,
        "bytes": int(round(sampled_bytes / hits * row_count)),
        "sampledRows": hits,
        "partial": timed_out,
    }, timed_out


def _database_stats(conn: sqlite3.Connection, database_bytes: int) -> dict[str, int]:
    page_size = _pragma_int(conn, "page_size") or 4096
    page_count = _pragma_int(conn, "page_count")
    freelist_bytes = _pragma_int(conn, "freelist_count") * page_size
    return {
        "databaseBytes": database_bytes,
        "pageSize": page_size,
        "pageCount": page_count,
        "freelistBytes": freelist_bytes,
        "allocatedBytes": max(0, page_count * page_size - freelist_bytes),
    }


def _exact_rows(table_names: list[str], exact: dict[str, int]) -> list[dict[str, Any]]:
    return [
        {"name": name, "rowCount": None, "bytes": exact.get(name, 0), "sampledRows": 0}
        for name in table_names
    ]


def _sample_rows(
    conn: sqlite3.Connection,
    table_names: list[str],
    *,
    samples: int,
    deadline: float,
) -> tuple[list[dict[str, Any]], list[str], bool]:
    rng = random.Random(_SAMPLE_SEED)
    rows: list[dict[str, Any]] = []
    skipped: list[str] = []
    complete = True
    for name in table_names:
        if time.monotonic() >= deadline:
            complete = False
            break
        estimate, timed_out = _estimate_table(
            conn, name, samples=samples, rng=rng, deadline=deadline
        )
        if estimate is not None:
            rows.append(estimate)
        elif not timed_out:
            skipped.append(name)
        if timed_out:
            complete = False
            break
    return rows, skipped, complete


def _shown_rows(
    rows: list[dict[str, Any]],
    *,
    allocated_bytes: int,
    max_tables: int,
) -> tuple[list[dict[str, Any]], int, bool]:
    rows.sort(key=lambda row: int(row["bytes"]), reverse=True)
    table_bytes = sum(int(row["bytes"]) for row in rows)
    shown = rows[: max(1, int(max_tables))]
    for row in shown:
        row["share"] = round(int(row["bytes"]) / max(1, allocated_bytes), 4)
    return shown, max(0, allocated_bytes - table_bytes), len(rows) > len(shown)


def measure_table_sizes(
    db_path: Path | str,
    *,
    sample_rows: int = DEFAULT_SAMPLE_ROWS_PER_TABLE,
    max_tables: int = DEFAULT_MAX_TABLES,
    budget_ms: int = DEFAULT_BUDGET_MS,
    exact_max_bytes: int = DEFAULT_EXACT_MAX_DATABASE_BYTES,
) -> dict[str, Any]:
    """Return a bounded per-table space breakdown for one runtime database."""
    started = time.monotonic()
    budget_ms = max(0, int(budget_ms))
    deadline = started + budget_ms / 1000.0
    path = Path(db_path)
    result: dict[str, Any] = {
        "schemaVersion": 1,
        "status": "unavailable",
        "method": "",
        "capturedAt": _now_iso(),
        "durationMs": 0,
        "budgetMs": budget_ms,
        "databaseBytes": 0,
        "pageSize": 0,
        "pageCount": 0,
        "allocatedBytes": 0,
        "freelistBytes": 0,
        "estimated": True,
        "complete": False,
        "sampledTables": 0,
        "skippedTables": [],
        "tables": [],
        "otherBytes": 0,
        "truncated": False,
        "error": "",
    }
    if not path.is_file():
        result["error"] = "database file not found"
        result["durationMs"] = int(round((time.monotonic() - started) * 1000))
        return result

    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        conn.execute("PRAGMA query_only = ON")
        database_bytes = path.stat().st_size
        stats = _database_stats(conn, database_bytes)
        result.update(stats)
        names = _table_names(conn)
        exact: dict[str, int] | None = None
        if database_bytes <= max(0, int(exact_max_bytes)) and time.monotonic() < deadline:
            exact = _measure_exact(conn, names)
        if exact is not None:
            rows = _exact_rows(names, exact)
            result.update(
                status="ok",
                method="dbstat",
                estimated=False,
                complete=True,
                sampledTables=len(names),
            )
        else:
            rows, skipped, complete = _sample_rows(
                conn, names, samples=max(1, int(sample_rows)), deadline=deadline
            )
            result.update(
                status="ok" if complete else "partial",
                method="rowid-sample",
                estimated=True,
                complete=complete,
                sampledTables=len(rows),
                skippedTables=skipped,
            )
        shown, other_bytes, truncated = _shown_rows(
            rows, allocated_bytes=stats["allocatedBytes"], max_tables=max_tables
        )
        result["tables"] = shown
        result["otherBytes"] = other_bytes
        result["truncated"] = truncated
    except sqlite3.Error as exc:
        result["status"] = "unavailable"
        result["error"] = str(exc)
    finally:
        if conn is not None:
            conn.close()
        result["durationMs"] = int(round((time.monotonic() - started) * 1000))
    return result
