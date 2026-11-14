"""Rebuild the runtime database around the current jobs feed generation.

A long-lived runtime database keeps every historical jobs-feed generation.
Deleting those rows in bounded batches is correct but slow: on the live store it
needs days to reach a size where ``VACUUM`` is useful. This leaf builds a fresh
database that contains every non-feed table verbatim and only the published
generation of ``jobs`` / ``job_sources``, validates it against the source feed
contract, and leaves the swap to :class:`~src.storage.baluffo_store.BaluffoStore`.

The rebuild never deletes anything from the live database: the caller installs
the validated staging copy with a rollback hardlink.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from src.shared.json_io import json_dumps, loads_object
from src.shared.utils import coerce_non_negative_int
from src.storage.job_runtime import JobRuntimeStore, jobs_feed_rows_hash
from src.storage.retention_cancel import CancelRequested, raise_if_cancelled
from src.storage_metrics import file_size

FEED_TABLES = ("jobs", "job_sources")
FEED_STATE_TABLE = "job_feed_state"
REBUILD_JOURNAL_SUFFIX = ".rebuild.json"
REBUILD_STAGING_SUFFIX = ".rebuild.tmp"
REBUILD_BACKUP_SUFFIX = ".pre-rebuild"
# The live database is 226 GB with a current feed of roughly 200k rows. A
# rebuilt copy is expected in the 0.5-1.5 GB range; 1-3 GB is the planning
# envelope. A small database still gets a conservative floor so the free-space
# gate is meaningful in tests and on fresh installs.
DEFAULT_REBUILD_MIN_FREE_BYTES = 256 * 1024 * 1024
DEFAULT_REBUILD_MAX_PLANNED_BYTES = 3 * 1024 * 1024 * 1024
DEFAULT_REBUILD_FREE_SPACE_MARGIN_BYTES = 64 * 1024 * 1024
_PROGRESS_INTERVAL_S = 1.0


class CurrentFeedRebuildError(RuntimeError):
    """Raised when a current-feed rebuild cannot produce a trustworthy copy."""


class CurrentFeedRebuildCancelled(CurrentFeedRebuildError):
    """Raised when the operator stopped the rebuild before installation."""


def rebuild_journal_path(db_path: Path | str) -> Path:
    path = Path(db_path)
    return path.with_name(f"{path.name}{REBUILD_JOURNAL_SUFFIX}")


def rebuild_backup_path(db_path: Path | str) -> Path:
    path = Path(db_path)
    return path.with_name(f"{path.name}{REBUILD_BACKUP_SUFFIX}")


def rebuild_staging_path(db_path: Path | str, *, run_id: str = "") -> Path:
    path = Path(db_path)
    token = "".join(character for character in str(run_id or "") if character.isalnum())[-16:]
    token = token or f"{os.getpid()}-{int(time.time() * 1000)}"
    return path.with_name(f"{path.name}.{token}{REBUILD_STAGING_SUFFIX}")


def rebuild_required_free_bytes(database_bytes: int) -> int:
    """Free space a staging copy must have before it starts.

    Using a percentage of the live file keeps the gate proportional without
    traversing 226 GB to measure the current feed: 1% of the live file is
    2.26 GB at the observed size, inside the measured 1-3 GB envelope.
    """
    proportional = max(0, int(database_bytes)) // 100
    planned = min(
        DEFAULT_REBUILD_MAX_PLANNED_BYTES,
        max(DEFAULT_REBUILD_MIN_FREE_BYTES, proportional),
    )
    return planned + DEFAULT_REBUILD_FREE_SPACE_MARGIN_BYTES


def read_rebuild_journal(path: Path | str) -> dict[str, Any]:
    journal_path = Path(path)
    try:
        payload = json.loads(journal_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_rebuild_journal(path: Path | str, payload: dict[str, Any]) -> None:
    """Persist the crash journal atomically, best-effort.

    The journal is evidence, not control flow: a failed write must not abort a
    rebuild whose database steps are already valid.
    """
    journal_path = Path(path)
    temp_path = journal_path.with_name(f"{journal_path.name}.tmp")
    try:
        temp_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temp_path, journal_path)
    except OSError:
        temp_path.unlink(missing_ok=True)


def update_rebuild_journal(path: Path | str, **fields: Any) -> dict[str, Any]:
    payload = read_rebuild_journal(path)
    payload.update(fields)
    payload["updatedAt"] = fields.get("updatedAt") or _now_iso()
    write_rebuild_journal(path, payload)
    return payload


def _now_iso() -> str:
    from src.shared.utils import now_iso

    return now_iso()


def _quote(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _connect_read_only(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        f"{path.resolve().as_uri()}?mode=ro",
        uri=True,
        timeout=30.0,
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    return conn


def _schema_objects(conn: sqlite3.Connection) -> dict[str, list[str]]:
    rows = conn.execute(
        """
        SELECT type, name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
        ORDER BY rowid ASC
        """
    ).fetchall()
    objects: dict[str, list[str]] = {"table": [], "index": [], "trigger": [], "view": []}
    for row in rows:
        object_type = str(row["type"] or "")
        sql = str(row["sql"] or "")
        if object_type not in objects:
            raise CurrentFeedRebuildError(f"unsupported schema object type: {object_type}")
        if object_type == "table" and sql.lstrip().upper().startswith("CREATE VIRTUAL TABLE"):
            raise CurrentFeedRebuildError(
                f"virtual table {row['name']!r} is not supported by the current-feed rebuild"
            )
        objects[object_type].append(sql)
    return objects


def _table_names(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY rowid ASC
        """
    ).fetchall()
    return [str(row["name"]) for row in rows]


def _count(conn: sqlite3.Connection, table: str, *, generation: str = "") -> int:
    sql = f"SELECT COUNT(*) FROM {_quote(table)}"
    parameters: tuple[Any, ...] = ()
    if generation and table in FEED_TABLES:
        sql += " WHERE feed_generation = ?"
        parameters = (generation,)
    row = conn.execute(sql, parameters).fetchone()
    return coerce_non_negative_int(row[0] if row is not None else 0)


def _feed_state(conn: sqlite3.Connection) -> dict[str, Any]:
    try:
        row = conn.execute(f"SELECT * FROM {_quote(FEED_STATE_TABLE)} WHERE id = 1").fetchone()
    except sqlite3.Error:
        return {}
    return dict(row) if row is not None else {}


def _json_loads_object(value: Any) -> dict[str, Any]:
    return loads_object(value)


def _source_hash(conn: sqlite3.Connection, generation: str) -> str:
    rows = conn.execute(
        """
        SELECT payload_json
        FROM job_sources
        WHERE feed_generation = ?
        ORDER BY job_key ASC, source_ordinal ASC
        """,
        (generation,),
    ).fetchall()
    payloads = [_json_loads_object(row[0]) for row in rows]
    return hashlib.sha256(json_dumps(payloads).encode("utf-8")).hexdigest()


def _row_hash(conn: sqlite3.Connection, generation: str) -> str:
    adapter = _ReadOnlyStoreAdapter(conn)
    rows = JobRuntimeStore(cast(Any, adapter)).rows_for_generation(generation)
    return jobs_feed_rows_hash(rows)


class _ReadOnlyStoreAdapter:
    """The two read primitives ``JobRuntimeStore.rows_for_generation`` needs."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def execute_read(
        self, sql: str, parameters: tuple[Any, ...] | list[Any] = ()
    ) -> list[dict[str, Any]]:
        return [dict(row) for row in self._conn.execute(sql, parameters).fetchall()]

    def execute_scalar(self, sql: str, parameters: tuple[Any, ...] | list[Any] = ()) -> Any:
        row = self._conn.execute(sql, parameters).fetchone()
        return None if row is None else row[0]


def database_feed_state(path: Path | str) -> dict[str, Any]:
    """Read the published feed state without counting any table.

    The live database is hundreds of gigabytes of historical generations, so the
    rebuild start path must not run an exact ``COUNT(*)`` over ``jobs`` /
    ``job_sources`` just to learn the one generation `job_feed_state` names.
    """
    conn = _connect_read_only(Path(path))
    try:
        state = _feed_state(conn)
    finally:
        conn.close()
    return {
        "currentGeneration": str(state.get("current_generation") or "").strip(),
        "jobRowCount": int(state.get("row_count") or 0),
        "sourceRowCount": int(state.get("source_count") or 0),
        "feedState": state,
    }


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({_quote(table)})").fetchall()
    return [str(row["name"]) for row in rows]


def _copy_table(
    source_conn: sqlite3.Connection,
    target_conn: sqlite3.Connection,
    table: str,
    *,
    generation: str,
    on_progress: Callable[[dict[str, Any]], None] | None,
    cancel_requested: CancelRequested | None,
    batch_size: int = 2000,
) -> int:
    """Copy one table in bounded batches.

    A read-only ``ATTACH`` into a WAL database is not portable (Windows fails it
    with a disk I/O error even while the store's reader works), so rows move
    through the already-open read-only connection. The copy stays inside one
    target transaction; batches only bound memory and give cancellation a point
    between them.
    """
    raise_if_cancelled(cancel_requested)
    columns = _table_columns(target_conn, table)
    if not columns:
        raise CurrentFeedRebuildError(f"table {table!r} has no columns")
    insert_sql = (
        f"INSERT INTO {_quote(table)} ({', '.join(_quote(column) for column in columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})"
    )
    select_sql = f"SELECT {', '.join(_quote(column) for column in columns)} FROM {_quote(table)}"
    parameters: tuple[Any, ...] = ()
    if table in FEED_TABLES:
        select_sql += " WHERE feed_generation = ?"
        parameters = (generation,)
    cursor = source_conn.execute(select_sql, parameters)
    copied = 0
    while True:
        raise_if_cancelled(cancel_requested)
        batch = cursor.fetchmany(max(1, int(batch_size)))
        if not batch:
            break
        target_conn.executemany(insert_sql, [tuple(row) for row in batch])
        copied += len(batch)
        if on_progress is not None:
            on_progress(
                {
                    "phase": "copy-table",
                    "table": table,
                    "tableRowsDone": copied,
                }
            )
    return copied


def rebuild_current_feed_database(
    source_path: Path | str,
    staging_path: Path | str,
    *,
    journal_path: Path | str | None = None,
    run_id: str = "",
    on_progress: Callable[[dict[str, Any]], None] | None = None,
    cancel_requested: CancelRequested | None = None,
) -> dict[str, Any]:
    """Build ``staging_path`` from ``source_path`` around the current generation.

    The copy is read-only against the live database. It returns only after the
    staging file passes ``quick_check``, ``foreign_key_check``, per-table count
    comparison, and the published feed count/hash validation.
    """
    source = Path(source_path).resolve()
    staging = Path(staging_path)
    journal = Path(journal_path) if journal_path is not None else rebuild_journal_path(source)
    started = time.monotonic()
    if not source.is_file():
        raise CurrentFeedRebuildError(f"runtime database not found: {source}")
    staging.unlink(missing_ok=True)
    write_rebuild_journal(
        journal,
        {
            "schemaVersion": 1,
            "runId": str(run_id or ""),
            "status": "running",
            "phase": "prepare",
            "sourcePath": str(source),
            "stagingPath": str(staging),
            "startedAt": _now_iso(),
            "updatedAt": _now_iso(),
            "error": "",
        },
    )

    def report(phase: str, **fields: Any) -> None:
        if on_progress is None:
            return
        on_progress(
            {
                "phase": phase,
                "elapsedMs": int(round((time.monotonic() - started) * 1000)),
                "stagingBytes": file_size(staging),
                **fields,
            }
        )

    source_conn = _connect_read_only(source)
    target_conn: sqlite3.Connection | None = None
    last_progress_at = 0.0

    def close_target() -> None:
        nonlocal target_conn
        if target_conn is None:
            return
        try:
            target_conn.close()
        except sqlite3.Error:
            return
        finally:
            target_conn = None

    def interrupt_check() -> int:
        nonlocal last_progress_at
        if cancel_requested is not None and cancel_requested():
            return 1
        now = time.monotonic()
        if on_progress is not None and now - last_progress_at >= _PROGRESS_INTERVAL_S:
            last_progress_at = now
            on_progress(
                {
                    "phase": "copy",
                    "elapsedMs": int(round((now - started) * 1000)),
                    "stagingBytes": file_size(staging),
                }
            )
        return 0

    try:
        state = _feed_state(source_conn)
        generation = str(state.get("current_generation") or "").strip()
        if not generation:
            raise CurrentFeedRebuildError("no current feed generation is published")
        expected_job_rows = int(state.get("row_count") or 0)
        expected_source_rows = int(state.get("source_count") or 0)
        expected_row_hash = str(state.get("row_hash") or "")
        expected_source_hash = str(state.get("source_hash") or "")

        page_size = int(source_conn.execute("PRAGMA page_size").fetchone()[0] or 4096)
        schema = _schema_objects(source_conn)
        table_names = _table_names(source_conn)
        if not table_names:
            raise CurrentFeedRebuildError("source database has no tables")
        non_feed_tables = [table for table in table_names if table not in FEED_TABLES]
        expected_table_counts = {table: _count(source_conn, table) for table in non_feed_tables}
        expected_table_counts[FEED_TABLES[0]] = expected_job_rows
        expected_table_counts[FEED_TABLES[1]] = expected_source_rows
        report("prepare", currentGeneration=generation, tablesTotal=len(table_names))

        target_conn = sqlite3.connect(str(staging), isolation_level=None, timeout=30.0)
        target_conn.row_factory = sqlite3.Row
        target_conn.execute("PRAGMA journal_mode=OFF")
        target_conn.execute("PRAGMA synchronous=OFF")
        target_conn.execute("PRAGMA foreign_keys=OFF")
        target_conn.execute("PRAGMA locking_mode=EXCLUSIVE")
        target_conn.execute("PRAGMA cache_size=-65536")
        target_conn.execute(f"PRAGMA page_size={max(512, page_size)}")
        target_conn.execute("BEGIN IMMEDIATE")
        for sql in schema["table"]:
            target_conn.execute(sql)
        target_conn.execute("COMMIT")
        raise_if_cancelled(cancel_requested)
        report("schema", currentGeneration=generation, tablesTotal=len(table_names))

        target_conn.set_progress_handler(interrupt_check, 10_000)
        target_conn.execute("BEGIN IMMEDIATE")
        copied = 0
        for table in table_names:
            _copy_table(
                source_conn,
                target_conn,
                table,
                generation=generation,
                on_progress=on_progress,
                cancel_requested=cancel_requested,
            )
            copied += 1
            report(
                "copy-table",
                table=table,
                tablesDone=copied,
                tablesTotal=len(table_names),
            )
        # AUTOINCREMENT tables create sqlite_sequence implicitly. Copying it keeps
        # the next rowid of task/registry tables monotonic across the rebuild.
        if "sqlite_sequence" in {
            str(row[0])
            for row in source_conn.execute(
                "SELECT name FROM sqlite_master WHERE name = 'sqlite_sequence'"
            ).fetchall()
        }:
            target_conn.executemany(
                "INSERT OR REPLACE INTO sqlite_sequence(name, seq) VALUES (?, ?)",
                [
                    (str(row[0]), int(row[1] or 0))
                    for row in source_conn.execute(
                        "SELECT name, seq FROM sqlite_sequence"
                    ).fetchall()
                ],
            )
        target_conn.execute("COMMIT")
        target_conn.set_progress_handler(None, 0)
        raise_if_cancelled(cancel_requested)
        report("indexes", tablesDone=len(table_names), tablesTotal=len(table_names))
        target_conn.execute("BEGIN IMMEDIATE")
        for sql in schema["index"]:
            target_conn.execute(sql)
        for sql in schema["trigger"]:
            target_conn.execute(sql)
        for sql in schema["view"]:
            target_conn.execute(sql)
        target_conn.execute("COMMIT")
        report("validate")
        target_conn.close()
        target_conn = None

        validation = validate_rebuilt_database(
            staging,
            current_generation=generation,
            expected_row_count=expected_job_rows,
            expected_row_hash=expected_row_hash,
            expected_source_count=expected_source_rows,
            expected_source_hash=expected_source_hash,
            expected_table_counts=expected_table_counts,
        )
    except sqlite3.OperationalError as exc:
        cancelled = (
            "interrupt" in str(exc).lower()
            and cancel_requested is not None
            and bool(cancel_requested())
        )
        close_target()
        staging.unlink(missing_ok=True)
        if cancelled:
            update_rebuild_journal(
                journal,
                status="cancelled",
                phase="cancelled",
                error="",
            )
            raise CurrentFeedRebuildCancelled("current-feed rebuild cancelled") from exc
        update_rebuild_journal(journal, status="failed", phase="failed", error=str(exc))
        raise CurrentFeedRebuildError(f"current-feed rebuild failed: {exc}") from exc
    except BaseException as exc:
        cancelled = cancel_requested is not None and bool(cancel_requested())
        close_target()
        staging.unlink(missing_ok=True)
        update_rebuild_journal(
            journal,
            status="cancelled" if cancelled else "failed",
            phase="cancelled" if cancelled else "failed",
            error="" if cancelled else str(exc),
        )
        raise
    finally:
        close_target()
        source_conn.close()

    result = {
        "status": "complete",
        "phase": "complete",
        "currentGeneration": generation,
        "sourceBytes": file_size(source),
        "stagingBytes": file_size(staging),
        "durationMs": int(round((time.monotonic() - started) * 1000)),
        **validation,
    }
    update_rebuild_journal(
        journal,
        status="staged",
        phase="staged",
        currentGeneration=generation,
        stagingBytes=result["stagingBytes"],
        rowHash=validation["rowHash"],
        sourceHash=validation["sourceHash"],
        error="",
    )
    return result


def validate_rebuilt_database(
    target_path: Path | str,
    *,
    current_generation: str,
    expected_row_count: int,
    expected_row_hash: str,
    expected_source_count: int,
    expected_source_hash: str,
    expected_table_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Validate a staged copy against the published feed contract."""
    target = Path(target_path)
    conn = _connect_read_only(target)
    try:
        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0] or "")
        if quick_check != "ok":
            raise CurrentFeedRebuildError(f"rebuilt database quick_check failed: {quick_check}")
        inserted = conn.execute("PRAGMA foreign_key_check").fetchall()
        if inserted:
            raise CurrentFeedRebuildError(
                f"rebuilt database has {len(inserted)} foreign-key violations"
            )
        state = _feed_state(conn)
        staged_generation = str(state.get("current_generation") or "").strip()
        if staged_generation != current_generation:
            raise CurrentFeedRebuildError(
                "rebuilt database feed state does not point at the published generation"
            )
        job_rows = _count(conn, "jobs", generation=current_generation)
        source_rows = _count(conn, "job_sources", generation=current_generation)
        if job_rows != int(expected_row_count):
            raise CurrentFeedRebuildError(
                f"rebuilt database has {job_rows} current jobs, expected {expected_row_count}"
            )
        if source_rows != int(expected_source_count):
            raise CurrentFeedRebuildError(
                "rebuilt database has "
                f"{source_rows} current source rows, expected {expected_source_count}"
            )
        row_hash = _row_hash(conn, current_generation)
        source_hash = _source_hash(conn, current_generation)
        if expected_row_hash and row_hash != expected_row_hash:
            raise CurrentFeedRebuildError(
                "rebuilt database current-feed row hash does not match the published feed"
            )
        if expected_source_hash and source_hash != expected_source_hash:
            raise CurrentFeedRebuildError(
                "rebuilt database current-feed source hash does not match the published feed"
            )
        table_counts: dict[str, int] = {}
        if expected_table_counts:
            tables = set(_table_names(conn))
            for table, expected_count in expected_table_counts.items():
                if table not in tables:
                    raise CurrentFeedRebuildError(f"rebuilt database is missing table {table!r}")
                actual = _count(conn, table)
                table_counts[table] = actual
                if actual != int(expected_count):
                    raise CurrentFeedRebuildError(
                        f"rebuilt database table {table!r} has {actual} rows, "
                        f"expected {expected_count}"
                    )
        return {
            "quickCheck": quick_check,
            "foreignKeyViolations": 0,
            "currentGeneration": staged_generation,
            "jobRowCount": job_rows,
            "sourceRowCount": source_rows,
            "rowHash": row_hash,
            "sourceHash": source_hash,
            "tableCounts": table_counts,
            "validatedAt": _now_iso(),
        }
    finally:
        conn.close()


def free_disk_bytes(path: Path | str) -> int:
    target = Path(path)
    try:
        return int(shutil.disk_usage(target if target.exists() else target.parent).free)
    except OSError:
        return 0
