"""SQLite/WAL runtime storage skeleton for bridge-owned hot state.

AI boundary owns: SQLite connection, migration, WAL, transaction, and batch helpers for runtime storage.
AI boundary implement in: this file for store primitives; domain row APIs stay in sibling storage modules.
AI boundary search before contracts: storage domain modules, migrations, and runtime storage tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused storage runtime tests.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
import traceback
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar, cast

from src.storage_metrics import record_slow_store_write

LOGGER = logging.getLogger(__name__)

DEFAULT_DB_NAME = "baluffo-runtime.db"
DEFAULT_BUSY_TIMEOUT_MS = 30_000
DEFAULT_BUSY_RETRY_ATTEMPTS = 10
DEFAULT_BUSY_RETRY_BASE_MS = 10
DEFAULT_BUSY_RETRY_MAX_MS = 5_000
DEFAULT_BATCH_SIZE = 500
DEFAULT_WAL_CHECKPOINT_THRESHOLD_BYTES = 64 * 1024 * 1024
DEFAULT_WAL_TRUNCATE_THRESHOLD_BYTES = 256 * 1024 * 1024
DEFAULT_WAL_JOURNAL_SIZE_LIMIT_BYTES = DEFAULT_WAL_CHECKPOINT_THRESHOLD_BYTES
DEFAULT_QUICK_CHECK_MAX_DATABASE_BYTES = 64 * 1024 * 1024
DEFAULT_VACUUM_FREELIST_THRESHOLD_BYTES = 50 * 1024 * 1024
DEFAULT_VACUUM_FREELIST_RATIO = 0.20
# A VACUUM rewrites the whole database, so its cost and its write-lock hold time
# scale with file size. Past this size the store asks for more free space before
# rebuilding: a multi-gigabyte database that VACUUMs at 20% free pays a very long
# lock to reclaim a merely moderate freelist.
DEFAULT_VACUUM_LARGE_DATABASE_BYTES = 512 * 1024 * 1024
DEFAULT_VACUUM_LARGE_DATABASE_RATIO = 0.35
# The rebuilt database is only worth the lock while nobody is writing to it.
# The worker waits for the write path to be quiet this long, for at most the
# larger max-wait budget; a busy store records the skip instead of stalling
# writers behind the rebuild.
DEFAULT_VACUUM_IDLE_S = 30.0
# A rebuild of a database past vacuum_large_database_bytes holds the lock for tens
# of seconds, so it asks for proportionally stronger evidence that the write path
# is done: a bursty fetch can leave a 30 s gap between sources mid-run.
DEFAULT_VACUUM_LARGE_DATABASE_IDLE_S = 120.0
DEFAULT_VACUUM_IDLE_MAX_WAIT_S = 600.0
DEFAULT_VACUUM_IDLE_POLL_S = 5.0
DEFAULT_POST_SERVE_MAINTENANCE_GRACE_S = 5.0
# One SQLite write transaction should take tens of milliseconds. A write that
# waits longer than this to take the store write lock, or holds it longer, is
# recorded with its thread and stack: that is the only evidence that attributes a
# multi-second startup or route stall to the writer that caused it.
DEFAULT_SLOW_WRITE_WARN_MS = 1000
SLOW_WRITE_STACK_LIMIT = 12
MAINTENANCE_STATUS_DEFERRED = "deferred"
MAINTENANCE_STATUS_NOT_NEEDED = "not-needed"
MAINTENANCE_STATUS_SKIPPED_BUSY = "skipped-busy"
MAINTENANCE_STATUS_HELD_ACTIVE_TASK = "held-active-task"
MIGRATIONS_PACKAGE = "src.storage.migrations"
STORAGE_SCHEMA_VERSION = 1

DEFAULT_AUTHORITY_MODES = {
    "taskRuns": "sqlite",
    "taskEvents": "sqlite",
    "syncRuns": "sqlite",
    "sourceRuns": "sqlite",
    "jobsFeed": "sqlite",
    "sourceRegistry": "sqlite",
}

_T = TypeVar("_T")


class BaluffoStoreError(RuntimeError):
    """Raised when the runtime store cannot satisfy a required storage operation."""


@dataclass(frozen=True)
class _Migration:
    version: str
    name: str
    sql: str


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _is_busy_error(exc: sqlite3.OperationalError) -> bool:
    text = str(exc).lower()
    return "database is locked" in text or "database table is locked" in text or "busy" in text


def _split_sql_script(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


class BaluffoStore:
    """Owns the SQLite runtime database connection and migration lifecycle."""

    def __init__(
        self,
        data_dir: Path | str,
        *,
        db_name: str = DEFAULT_DB_NAME,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
        busy_retry_attempts: int = DEFAULT_BUSY_RETRY_ATTEMPTS,
        busy_retry_base_ms: int = DEFAULT_BUSY_RETRY_BASE_MS,
        busy_retry_max_ms: int = DEFAULT_BUSY_RETRY_MAX_MS,
        wal_checkpoint_threshold_bytes: int = DEFAULT_WAL_CHECKPOINT_THRESHOLD_BYTES,
        wal_truncate_threshold_bytes: int = DEFAULT_WAL_TRUNCATE_THRESHOLD_BYTES,
        quick_check_max_database_bytes: int = DEFAULT_QUICK_CHECK_MAX_DATABASE_BYTES,
        vacuum_freelist_threshold_bytes: int = DEFAULT_VACUUM_FREELIST_THRESHOLD_BYTES,
        vacuum_freelist_ratio: float = DEFAULT_VACUUM_FREELIST_RATIO,
        vacuum_large_database_bytes: int = DEFAULT_VACUUM_LARGE_DATABASE_BYTES,
        vacuum_large_database_ratio: float = DEFAULT_VACUUM_LARGE_DATABASE_RATIO,
        vacuum_idle_s: float = DEFAULT_VACUUM_IDLE_S,
        vacuum_large_database_idle_s: float = DEFAULT_VACUUM_LARGE_DATABASE_IDLE_S,
        vacuum_idle_max_wait_s: float = DEFAULT_VACUUM_IDLE_MAX_WAIT_S,
        vacuum_idle_poll_s: float = DEFAULT_VACUUM_IDLE_POLL_S,
        defer_maintenance: bool = False,
        compaction_hold: bool = False,
        slow_write_warn_ms: int = DEFAULT_SLOW_WRITE_WARN_MS,
    ) -> None:
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.db_name = str(db_name or DEFAULT_DB_NAME)
        self.db_path = self.data_dir / self.db_name
        self.busy_timeout_ms = max(1, int(busy_timeout_ms))
        self.busy_retry_attempts = max(1, int(busy_retry_attempts))
        self.busy_retry_base_ms = max(1, int(busy_retry_base_ms))
        self.busy_retry_max_ms = max(self.busy_retry_base_ms, int(busy_retry_max_ms))
        self.wal_checkpoint_threshold_bytes = max(1, int(wal_checkpoint_threshold_bytes))
        self.wal_truncate_threshold_bytes = max(
            self.wal_checkpoint_threshold_bytes, int(wal_truncate_threshold_bytes)
        )
        self.quick_check_max_database_bytes = max(1, int(quick_check_max_database_bytes))
        self.vacuum_freelist_threshold_bytes = max(1, int(vacuum_freelist_threshold_bytes))
        self.vacuum_freelist_ratio = max(0.01, min(1.0, float(vacuum_freelist_ratio)))
        self.vacuum_large_database_bytes = max(1, int(vacuum_large_database_bytes))
        self.vacuum_large_database_ratio = max(
            self.vacuum_freelist_ratio, min(1.0, float(vacuum_large_database_ratio))
        )
        self.vacuum_idle_s = max(0.0, float(vacuum_idle_s))
        self.vacuum_large_database_idle_s = max(
            self.vacuum_idle_s, float(vacuum_large_database_idle_s)
        )
        self.vacuum_idle_max_wait_s = max(0.0, float(vacuum_idle_max_wait_s))
        self.vacuum_idle_poll_s = max(0.05, float(vacuum_idle_poll_s))
        self._last_write_monotonic = time.monotonic()
        self._last_write_error = ""
        self._busy_count = 0
        self._write_attempts = 0
        self._write_transactions = 0
        self._healthy = True
        self._last_checkpoint: dict[str, Any] = {
            "status": "not-run",
            "reason": "",
            "mode": "",
            "durationMs": 0,
            "error": "",
        }
        self._last_vacuum: dict[str, Any] = {
            "status": "not-run",
            "reason": "",
            "durationMs": 0,
            "reclaimedBytes": 0,
            "error": "",
        }
        self._maintenance_thread: threading.Thread | None = None
        self._write_lock = threading.RLock()
        self._read_lock = threading.RLock()
        self._maintenance_lock = threading.RLock()
        self._maintenance_deferred = bool(defer_maintenance)
        self._compaction_hold = bool(compaction_hold)
        self.slow_write_warn_ms = max(0, int(slow_write_warn_ms))
        self._post_serve_maintenance_started = False
        self._reader: sqlite3.Connection | None = None

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._writer = self._connect()
        self._initialize_writer()
        self._ensure_migration_table()
        self.run_migrations()
        self._seed_authority_modes()
        startup_check = self.startup_probe()
        if startup_check != "ok":
            self._healthy = False
            raise BaluffoStoreError(f"SQLite startup probe failed: {startup_check}")
        self.schedule_wal_maintenance_if_needed(reason="startup")

    def close(self) -> None:
        self._checkpoint_if_needed(reason="close", mode="PASSIVE")
        with self._read_lock:
            if self._reader is not None:
                self._reader.close()
                self._reader = None
        self._writer.close()

    def __enter__(self) -> BaluffoStore:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=self.busy_timeout_ms / 1000,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _initialize_writer(self) -> None:
        wal_mode = str(self._writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] or "").lower()
        if wal_mode != "wal":
            self._healthy = False
            raise BaluffoStoreError(f"Unable to enable SQLite WAL mode: {wal_mode}")
        self._writer.execute("PRAGMA synchronous=NORMAL")
        self._writer.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        self._writer.execute("PRAGMA foreign_keys=ON")
        self._writer.execute("PRAGMA wal_autocheckpoint=1000")
        self._writer.execute(f"PRAGMA journal_size_limit={DEFAULT_WAL_JOURNAL_SIZE_LIMIT_BYTES}")
        foreign_keys = int(self._writer.execute("PRAGMA foreign_keys").fetchone()[0] or 0)
        if foreign_keys != 1:
            self._healthy = False
            raise BaluffoStoreError("Unable to enable SQLite foreign key enforcement")

    def _read_connection(self) -> sqlite3.Connection:
        with self._read_lock:
            if self._reader is None:
                self._reader = self._connect()
            return self._reader

    def _ensure_migration_table(self) -> None:
        self.write(
            lambda conn: conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                )
                """
            )
        )

    def _load_migrations(self) -> list[_Migration]:
        from importlib import resources

        migration_root = resources.files(MIGRATIONS_PACKAGE)
        migrations: list[_Migration] = []
        for entry in sorted(migration_root.iterdir(), key=lambda item: item.name):
            if not entry.name.endswith(".sql"):
                continue
            version = entry.name.split("_", 1)[0]
            migrations.append(
                _Migration(version=version, name=entry.name, sql=entry.read_text(encoding="utf-8"))
            )
        if not migrations:
            raise BaluffoStoreError("No SQLite migrations found")
        return migrations

    def applied_migrations(self) -> list[str]:
        rows = self.execute_read("SELECT version FROM schema_migrations ORDER BY version")
        return [str(row["version"]) for row in rows]

    def migration_version(self) -> str:
        applied = self.applied_migrations()
        return applied[-1] if applied else ""

    def run_migrations(self) -> list[str]:
        applied = set(self.applied_migrations())
        applied_now: list[str] = []
        for migration in self._load_migrations():
            if migration.version in applied:
                continue

            def apply_migration(
                conn: sqlite3.Connection, migration: _Migration = migration
            ) -> None:
                for statement in _split_sql_script(migration.sql):
                    conn.execute(statement)
                conn.execute(
                    """
                    INSERT INTO schema_migrations(version, name, applied_at)
                    VALUES (?, ?, ?)
                    """,
                    (migration.version, migration.name, _now_iso()),
                )

            self.write(apply_migration)
            applied.add(migration.version)
            applied_now.append(migration.version)
        return applied_now

    def write(self, callback: Callable[[sqlite3.Connection], _T]) -> _T:
        last_busy_error: sqlite3.OperationalError | None = None
        for attempt in range(self.busy_retry_attempts):
            # Every attempt, including a busy retry, counts as write activity: the
            # idle gate must not start a rebuild while writers are still arriving.
            self._last_write_monotonic = time.monotonic()
            self._write_attempts += 1
            try:
                lock_started = time.perf_counter()
                with self._write_lock:
                    waited_s = time.perf_counter() - lock_started
                    hold_started = time.perf_counter()
                    self._writer.execute("BEGIN IMMEDIATE")
                    result = callback(self._writer)
                    self._writer.commit()
                    held_s = time.perf_counter() - hold_started
                self._record_slow_write(operation="write", waited_s=waited_s, held_s=held_s)
                self._last_write_error = ""
                self._write_transactions += 1
                self.schedule_wal_maintenance_if_needed(reason="write")
                self.schedule_compaction_if_needed(reason="write")
                return result
            except sqlite3.OperationalError as exc:
                self._rollback_after_error()
                if not _is_busy_error(exc):
                    self._healthy = False
                    self._last_write_error = str(exc)
                    raise
                last_busy_error = exc
                self._busy_count += 1
                self._last_write_error = str(exc)
                if attempt + 1 >= self.busy_retry_attempts:
                    self._healthy = False
                    break
                self._sleep_for_retry(attempt)
            except BaseException as exc:
                self._rollback_after_error()
                self._healthy = False
                self._last_write_error = str(exc)
                raise
        message = f"SQLite write failed after busy retry attempts: {last_busy_error}"
        LOGGER.error(message)
        raise BaluffoStoreError(message)

    def _record_slow_write(self, *, operation: str, waited_s: float, held_s: float) -> None:
        """Journal a write-lock operation that will not go unnoticed in the field.

        A waiter and a holder both write a row, so the stalled caller and the
        writer that blocked it are attributable from the same journal.
        """
        if self.slow_write_warn_ms <= 0:
            return
        wait_ms = int(round(max(0.0, waited_s) * 1000))
        hold_ms = int(round(max(0.0, held_s) * 1000))
        if wait_ms < self.slow_write_warn_ms and hold_ms < self.slow_write_warn_ms:
            return
        try:
            record_slow_store_write(
                operation=operation,
                wait_ms=wait_ms,
                hold_ms=hold_ms,
                thread_name=threading.current_thread().name,
                stack="".join(
                    traceback.format_list(traceback.extract_stack()[:-1][-SLOW_WRITE_STACK_LIMIT:])
                ),
                artifact=self.db_path.name,
                data_dir=self.data_dir,
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            # Diagnostics must never break a committed write.
            return

    def _rollback_after_error(self) -> None:
        try:
            self._writer.rollback()
        except sqlite3.Error:
            return

    def _sleep_for_retry(self, attempt: int) -> None:
        delay_ms = min(self.busy_retry_max_ms, self.busy_retry_base_ms * (2**attempt))
        time.sleep(delay_ms / 1000)

    def _read_with_retry(self, callback: Callable[[sqlite3.Connection], _T]) -> _T:
        last_busy_error: sqlite3.OperationalError | None = None
        for attempt in range(self.busy_retry_attempts):
            try:
                with self._read_lock:
                    result = callback(self._read_connection())
                if last_busy_error is not None:
                    self._last_write_error = ""
                return result
            except sqlite3.OperationalError as exc:
                if not _is_busy_error(exc):
                    raise
                last_busy_error = exc
                self._busy_count += 1
                self._last_write_error = str(exc)
                if attempt + 1 >= self.busy_retry_attempts:
                    self._healthy = False
                    break
                self._sleep_for_retry(attempt)
        message = f"SQLite read failed after busy retry attempts: {last_busy_error}"
        LOGGER.error(message)
        raise BaluffoStoreError(message)

    def _execute_read_fetchall(
        self,
        conn: sqlite3.Connection,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any],
    ) -> list[sqlite3.Row]:
        return conn.execute(sql, parameters).fetchall()

    def _execute_read_fetchone(
        self,
        conn: sqlite3.Connection,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any],
    ) -> sqlite3.Row | None:
        return cast(sqlite3.Row | None, conn.execute(sql, parameters).fetchone())

    def execute_read(
        self, sql: str, parameters: Sequence[Any] | dict[str, Any] = ()
    ) -> list[dict[str, Any]]:
        rows = self._read_with_retry(
            lambda conn: self._execute_read_fetchall(conn, sql, parameters)
        )
        return [dict(row) for row in rows]

    def execute_scalar(self, sql: str, parameters: Sequence[Any] | dict[str, Any] = ()) -> Any:
        row = self._read_with_retry(lambda conn: self._execute_read_fetchone(conn, sql, parameters))
        return None if row is None else row[0]

    def bulk_execute(
        self,
        sql: str,
        rows: Iterable[Sequence[Any] | dict[str, Any]],
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> int:
        batch_limit = max(1, int(batch_size))
        batch: list[Sequence[Any] | dict[str, Any]] = []
        total = 0

        def flush(current_batch: list[Sequence[Any] | dict[str, Any]]) -> None:
            self.write(lambda conn: conn.executemany(sql, current_batch))

        for row in rows:
            batch.append(row)
            if len(batch) >= batch_limit:
                flush(batch)
                total += len(batch)
                batch = []
        if batch:
            flush(batch)
            total += len(batch)
        return total

    def _seed_authority_modes(self) -> None:
        def seed(conn: sqlite3.Connection) -> None:
            for surface, mode in DEFAULT_AUTHORITY_MODES.items():
                conn.execute(
                    """
                    INSERT OR IGNORE INTO storage_authority_modes(
                        surface, mode, reason, updated_at
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (surface, mode, "default-sqlite-authority", _now_iso()),
                )

        self.write(seed)

    def get_authority_modes(self) -> dict[str, str]:
        rows = self.execute_read(
            "SELECT surface, mode FROM storage_authority_modes ORDER BY surface"
        )
        modes = {str(row["surface"]): str(row["mode"]) for row in rows}
        return {**DEFAULT_AUTHORITY_MODES, **modes}

    def set_authority_mode(self, surface: str, mode: str, *, reason: str = "") -> None:
        if surface not in DEFAULT_AUTHORITY_MODES:
            raise ValueError(f"Unknown storage authority surface: {surface}")
        if mode not in {"json", "shadow", "sqlite", "disabled"}:
            raise ValueError(f"Unsupported storage authority mode: {mode}")
        self.write(
            lambda conn: conn.execute(
                """
                INSERT INTO storage_authority_modes(surface, mode, reason, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(surface) DO UPDATE SET
                    mode = excluded.mode,
                    reason = excluded.reason,
                    updated_at = excluded.updated_at
                """,
                (surface, mode, str(reason or ""), _now_iso()),
            )
        )

    def quick_check(self, *, limit: int | None = None) -> str:
        if limit is None:
            sql = "PRAGMA quick_check"
        else:
            sql = f"PRAGMA quick_check({max(1, int(limit))})"
        result = str(self.execute_scalar(sql) or "")
        return result or "unknown"

    def startup_probe(self) -> str:
        try:
            self.execute_scalar("PRAGMA schema_version")
        except (BaluffoStoreError, sqlite3.Error) as exc:
            return str(exc) or "failed"
        return "ok"

    def quick_check_for_health(self) -> tuple[str, bool]:
        sizes = self.file_sizes()
        if int(sizes.get("databaseBytes") or 0) > self.quick_check_max_database_bytes:
            return "deferred-large-database", True
        return self.quick_check(limit=1), False

    def wal_mode(self) -> str:
        with self._write_lock:
            return str(self._writer.execute("PRAGMA journal_mode").fetchone()[0] or "").lower()

    def checkpoint_required(self, *, mode: str = "TRUNCATE") -> dict[str, int]:
        checkpoint_mode = mode.upper()
        if checkpoint_mode not in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}:
            raise ValueError(f"Unsupported SQLite checkpoint mode: {mode}")
        last_result: tuple[int, int, int] | None = None
        for attempt in range(self.busy_retry_attempts):
            try:
                row = self._execute_checkpoint(checkpoint_mode)
                last_result = (int(row[0] or 0), int(row[1] or 0), int(row[2] or 0))
                if last_result[0] == 0:
                    self._last_write_error = ""
                    return {
                        "busy": last_result[0],
                        "logFrames": last_result[1],
                        "checkpointedFrames": last_result[2],
                    }
                self._busy_count += 1
                self._last_write_error = f"SQLite WAL checkpoint busy: {last_result}"
                if attempt + 1 >= self.busy_retry_attempts:
                    self._healthy = False
                    break
                self._sleep_for_retry(attempt)
            except sqlite3.OperationalError as exc:
                if not _is_busy_error(exc):
                    self._healthy = False
                    self._last_write_error = str(exc)
                    raise
                self._busy_count += 1
                self._last_write_error = str(exc)
                if attempt + 1 >= self.busy_retry_attempts:
                    self._healthy = False
                    break
                self._sleep_for_retry(attempt)
        message = f"SQLite WAL checkpoint failed after busy retry attempts: {last_result}"
        LOGGER.error(message)
        raise BaluffoStoreError(message)

    def _execute_checkpoint(self, mode: str) -> sqlite3.Row:
        lock_started = time.perf_counter()
        with self._write_lock:
            waited_s = time.perf_counter() - lock_started
            hold_started = time.perf_counter()
            row = cast(
                sqlite3.Row, self._writer.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
            )
            held_s = time.perf_counter() - hold_started
        self._record_slow_write(
            operation=f"checkpoint:{str(mode or '').lower()}", waited_s=waited_s, held_s=held_s
        )
        return row

    @staticmethod
    def _path_size(path: Path) -> int:
        try:
            return max(0, int(path.stat().st_size))
        except OSError:
            return 0

    def freelist_info(self) -> dict[str, int]:
        try:
            freelist_count = int(self.execute_scalar("PRAGMA freelist_count") or 0)
            page_size = int(self.execute_scalar("PRAGMA page_size") or 4096)
            return {
                "freelistCount": freelist_count,
                "pageSize": page_size,
                "freelistBytes": freelist_count * page_size,
            }
        except (BaluffoStoreError, sqlite3.Error, OSError):
            return {"freelistCount": 0, "pageSize": 4096, "freelistBytes": 0}

    def file_sizes(self) -> dict[str, int]:
        freelist_bytes = self.freelist_info()["freelistBytes"]
        return {
            "databaseBytes": self._path_size(self.db_path),
            "walBytes": self._path_size(self.db_path.with_name(f"{self.db_path.name}-wal")),
            "shmBytes": self._path_size(self.db_path.with_name(f"{self.db_path.name}-shm")),
            "freelistBytes": freelist_bytes,
        }

    def wal_maintenance_status(self) -> dict[str, Any]:
        with self._maintenance_lock:
            running = self._maintenance_thread is not None and self._maintenance_thread.is_alive()
            return {
                **self._last_checkpoint,
                "running": running,
                "deferred": self._maintenance_deferred,
                "postServeRun": self._post_serve_maintenance_started,
                "checkpointThresholdBytes": self.wal_checkpoint_threshold_bytes,
                "truncateThresholdBytes": self.wal_truncate_threshold_bytes,
            }

    @property
    def maintenance_deferred(self) -> bool:
        with self._maintenance_lock:
            return self._maintenance_deferred

    @property
    def compaction_held(self) -> bool:
        with self._maintenance_lock:
            return self._compaction_hold

    def set_compaction_hold(self, held: bool) -> bool:
        """Hold the rebuild while bridge tasks are running, and release it after.

        Write silence alone is a weak signal during a fetch or sync: the worker can
        see a quiet gap between two sources and start a rebuild that the task then
        has to write behind. The bridge holds compaction while any task row is
        active and releases it when the last one goes terminal, which re-schedules
        a due rebuild through the idle gate instead of waiting for one.

        Returns True when the release scheduled a rebuild.
        """
        with self._maintenance_lock:
            changed = self._compaction_hold != bool(held)
            self._compaction_hold = bool(held)
        if not changed or self._compaction_hold:
            return False
        return self.schedule_compaction_if_needed(reason="compaction-hold-released")

    def set_maintenance_deferred(self, deferred: bool) -> None:
        """Hold or release background SQLite maintenance.

        A deferred store keeps serving reads and writes but never starts a
        checkpoint or VACUUM. Both take the store write lock for their whole run
        (a VACUUM on a bloated runtime database takes tens of seconds), so a
        startup write that lands behind one stalls every later startup step.
        """
        with self._maintenance_lock:
            self._maintenance_deferred = bool(deferred)

    def _record_deferred_maintenance(self, kind: str, *, reason: str, **fields: int) -> None:
        status = {
            "status": MAINTENANCE_STATUS_DEFERRED,
            "reason": str(reason or ""),
            "durationMs": 0,
            "error": "",
            **{key: int(value) for key, value in fields.items()},
        }
        with self._maintenance_lock:
            if kind == "wal":
                self._last_checkpoint = {**status, "mode": ""}
            else:
                self._last_vacuum = {**status, "reclaimedBytes": 0}

    def resume_maintenance_after_serving(
        self,
        *,
        reason: str = "post-serve",
        grace_s: float = DEFAULT_POST_SERVE_MAINTENANCE_GRACE_S,
    ) -> bool:
        """Re-enable maintenance once the server is answering requests.

        The released work runs on one background thread after a short grace so
        the first page load is not queued behind a VACUUM. It runs at most once
        per store instance; later writes schedule maintenance as before.
        """
        with self._maintenance_lock:
            if self._post_serve_maintenance_started:
                return False
            self._post_serve_maintenance_started = True
            self._maintenance_deferred = False
            thread = threading.Thread(
                target=self._run_post_serve_maintenance,
                kwargs={
                    "reason": str(reason or ""),
                    "grace_s": max(0.0, float(grace_s)),
                },
                name="baluffo-sqlite-post-serve-maintenance",
                daemon=True,
            )
            self._maintenance_thread = thread
            thread.start()
            return True

    def _run_post_serve_maintenance(self, *, reason: str, grace_s: float) -> None:
        if grace_s > 0.0:
            time.sleep(grace_s)
        try:
            with self._maintenance_lock:
                if self._last_checkpoint.get("status") == MAINTENANCE_STATUS_DEFERRED:
                    self._last_checkpoint = {
                        **self._last_checkpoint,
                        "status": MAINTENANCE_STATUS_NOT_NEEDED,
                        "reason": f"{reason}:released",
                    }
                if self._last_vacuum.get("status") == MAINTENANCE_STATUS_DEFERRED:
                    self._last_vacuum = {
                        **self._last_vacuum,
                        "status": MAINTENANCE_STATUS_NOT_NEEDED,
                        "reason": f"{reason}:released",
                    }
            sizes = self.file_sizes()
            wal_bytes = int(sizes.get("walBytes") or 0)
            if wal_bytes >= self.wal_checkpoint_threshold_bytes:
                mode = "TRUNCATE" if wal_bytes >= self.wal_truncate_threshold_bytes else "PASSIVE"
                self._run_wal_maintenance(reason=reason, mode=mode)
                sizes = self.file_sizes()
            if self._compaction_needed(sizes):
                self._run_compaction_when_idle(reason=reason, sizes=sizes)
        except (BaluffoStoreError, sqlite3.Error, OSError, ValueError) as exc:
            LOGGER.warning("SQLite post-serve maintenance failed: %s", exc)
        finally:
            with self._maintenance_lock:
                if self._maintenance_thread is threading.current_thread():
                    self._maintenance_thread = None

    def schedule_wal_maintenance_if_needed(self, *, reason: str = "") -> bool:
        sizes = self.file_sizes()
        wal_bytes = int(sizes.get("walBytes") or 0)
        if wal_bytes < self.wal_checkpoint_threshold_bytes:
            return False
        if self.maintenance_deferred:
            self._record_deferred_maintenance("wal", reason=reason, walBytes=wal_bytes)
            return False
        with self._maintenance_lock:
            if self._maintenance_thread is not None and self._maintenance_thread.is_alive():
                self._last_checkpoint = {
                    **self._last_checkpoint,
                    "status": "already-running",
                    "reason": str(reason or ""),
                    "walBytes": wal_bytes,
                }
                return False
            mode = "TRUNCATE" if wal_bytes >= self.wal_truncate_threshold_bytes else "PASSIVE"
            self._last_checkpoint = {
                "status": "scheduled",
                "reason": str(reason or ""),
                "mode": mode,
                "durationMs": 0,
                "error": "",
                **sizes,
            }
            thread = threading.Thread(
                target=self._run_wal_maintenance,
                kwargs={"reason": str(reason or ""), "mode": mode},
                name="baluffo-sqlite-wal-maintenance",
                daemon=True,
            )
            self._maintenance_thread = thread
            thread.start()
            return True

    def _checkpoint_if_needed(self, *, reason: str, mode: str) -> bool:
        sizes = self.file_sizes()
        wal_bytes = int(sizes.get("walBytes") or 0)
        if wal_bytes < self.wal_checkpoint_threshold_bytes:
            return False
        self._run_wal_maintenance(reason=reason, mode=mode)
        return True

    def _run_wal_maintenance(self, *, reason: str, mode: str) -> None:
        started = time.perf_counter()
        before = self.file_sizes()
        checkpoint_mode = str(mode or "PASSIVE").upper()
        try:
            result = self.checkpoint_required(mode=checkpoint_mode)
            after = self.file_sizes()
            status = {
                "status": "ok",
                "reason": str(reason or ""),
                "mode": checkpoint_mode,
                "durationMs": int(round((time.perf_counter() - started) * 1000)),
                "error": "",
                **before,
                "afterDatabaseBytes": int(after.get("databaseBytes") or 0),
                "afterWalBytes": int(after.get("walBytes") or 0),
                "afterShmBytes": int(after.get("shmBytes") or 0),
                **result,
            }
        except (BaluffoStoreError, sqlite3.Error, OSError, ValueError) as exc:
            status = {
                "status": "failed",
                "reason": str(reason or ""),
                "mode": checkpoint_mode,
                "durationMs": int(round((time.perf_counter() - started) * 1000)),
                "error": str(exc),
                **before,
            }
        with self._maintenance_lock:
            self._last_checkpoint = status

    def vacuum(self, *, reason: str = "") -> dict[str, Any]:
        started = time.perf_counter()
        before_bytes = self._path_size(self.db_path)
        last_busy_error: sqlite3.OperationalError | None = None
        for attempt in range(self.busy_retry_attempts):
            try:
                lock_started = time.perf_counter()
                with self._write_lock:
                    waited_s = time.perf_counter() - lock_started
                    hold_started = time.perf_counter()
                    self._writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    self._writer.execute("VACUUM")
                    self._writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    held_s = time.perf_counter() - hold_started
                self._record_slow_write(operation="vacuum", waited_s=waited_s, held_s=held_s)
                self._last_write_error = ""
                after_bytes = self._path_size(self.db_path)
                reclaimed = max(0, before_bytes - after_bytes)
                status = {
                    "status": "ok",
                    "reason": str(reason or ""),
                    "durationMs": int(round((time.perf_counter() - started) * 1000)),
                    "beforeBytes": before_bytes,
                    "afterBytes": after_bytes,
                    "reclaimedBytes": reclaimed,
                    "error": "",
                }
                with self._maintenance_lock:
                    self._last_vacuum = status
                return status
            except sqlite3.OperationalError as exc:
                if not _is_busy_error(exc):
                    self._healthy = False
                    self._last_write_error = str(exc)
                    raise
                last_busy_error = exc
                self._busy_count += 1
                self._last_write_error = str(exc)
                if attempt + 1 >= self.busy_retry_attempts:
                    self._healthy = False
                    break
                self._sleep_for_retry(attempt)
        message = f"SQLite VACUUM failed after busy retry attempts: {last_busy_error}"
        LOGGER.error(message)
        status = {
            "status": "failed",
            "reason": str(reason or ""),
            "durationMs": int(round((time.perf_counter() - started) * 1000)),
            "beforeBytes": before_bytes,
            "afterBytes": before_bytes,
            "reclaimedBytes": 0,
            "error": message,
        }
        with self._maintenance_lock:
            self._last_vacuum = status
        raise BaluffoStoreError(message)

    def compaction_status(self) -> dict[str, Any]:
        with self._maintenance_lock:
            return {
                **self._last_vacuum,
                "deferred": self._maintenance_deferred,
                "postServeRun": self._post_serve_maintenance_started,
                "freelistThresholdBytes": self.vacuum_freelist_threshold_bytes,
                "freelistRatio": self.vacuum_freelist_ratio,
                "largeDatabaseBytes": self.vacuum_large_database_bytes,
                "largeDatabaseFreelistRatio": self.vacuum_large_database_ratio,
                "largeDatabaseIdleS": self.vacuum_large_database_idle_s,
                "idleGateS": self.vacuum_idle_s,
                "idleMaxWaitS": self.vacuum_idle_max_wait_s,
                "heldByActiveTask": self._compaction_hold,
                "lastWriteAgoS": round(max(0.0, time.monotonic() - self._last_write_monotonic), 3),
            }

    def effective_vacuum_freelist_ratio(self, database_bytes: int) -> float:
        """The freelist ratio this database size must exceed before a rebuild."""
        if int(database_bytes) >= self.vacuum_large_database_bytes:
            return self.vacuum_large_database_ratio
        return self.vacuum_freelist_ratio

    def effective_vacuum_idle_s(self, database_bytes: int) -> float:
        """The write-silence window this database size must earn before a rebuild."""
        if int(database_bytes) >= self.vacuum_large_database_bytes:
            return self.vacuum_large_database_idle_s
        return self.vacuum_idle_s

    def _compaction_needed(self, sizes: dict[str, int]) -> bool:
        freelist_bytes = int(sizes.get("freelistBytes") or 0)
        database_bytes = int(sizes.get("databaseBytes") or 0)
        if freelist_bytes < self.vacuum_freelist_threshold_bytes:
            return False
        if database_bytes <= 0:
            return True
        return (freelist_bytes / database_bytes) >= self.effective_vacuum_freelist_ratio(
            database_bytes
        )

    def schedule_compaction_if_needed(self, *, reason: str = "") -> bool:
        sizes = self.file_sizes()
        if not self._compaction_needed(sizes):
            return False
        if self.maintenance_deferred:
            self._record_deferred_maintenance(
                "compaction",
                reason=reason,
                freelistBytes=int(sizes.get("freelistBytes") or 0),
                databaseBytes=int(sizes.get("databaseBytes") or 0),
            )
            return False
        with self._maintenance_lock:
            if self._maintenance_thread is not None and self._maintenance_thread.is_alive():
                return False
            thread = threading.Thread(
                target=self._run_compaction_maintenance,
                kwargs={"reason": str(reason or "")},
                name="baluffo-sqlite-compaction-maintenance",
                daemon=True,
            )
            self._maintenance_thread = thread
            thread.start()
            return True

    def _run_compaction_maintenance(self, *, reason: str) -> None:
        try:
            self._run_compaction_when_idle(reason=reason)
        except (BaluffoStoreError, sqlite3.Error, OSError, ValueError) as exc:
            LOGGER.warning("SQLite compaction maintenance failed: %s", exc)

    def _seconds_since_write(self) -> float:
        return max(0.0, time.monotonic() - self._last_write_monotonic)

    def _run_compaction_when_idle(
        self, *, reason: str, sizes: dict[str, int] | None = None
    ) -> bool:
        """Rebuild the database only once the write path has gone quiet.

        VACUUM holds the store write lock for its whole run, so starting it while
        writers are arriving queues them behind the whole rebuild. The worker waits
        for the size-aware quiet window (`effective_vacuum_idle_s`) of write
        silence, giving up after `vacuum_idle_max_wait_s` and recording
        `skipped-busy` so the skipped work stays visible in `/ops/storage-health`.
        A later write re-schedules it.
        """
        snapshot = dict(sizes) if sizes is not None else self.file_sizes()
        idle_gate_s = self.effective_vacuum_idle_s(int(snapshot.get("databaseBytes") or 0))
        waited_started = time.monotonic()
        deadline = waited_started + self.vacuum_idle_max_wait_s
        while True:
            if self.compaction_held:
                self._record_compaction_held(reason=reason, sizes=snapshot)
                return False
            idle_for_s = self._seconds_since_write()
            if idle_for_s >= idle_gate_s:
                self.vacuum(reason=reason)
                return True
            remaining_s = min(idle_gate_s - idle_for_s, deadline - time.monotonic())
            if remaining_s <= 0.0:
                self._record_compaction_skipped(
                    reason=reason,
                    sizes=snapshot,
                    waited_s=time.monotonic() - waited_started,
                    idle_for_s=idle_for_s,
                    idle_gate_s=idle_gate_s,
                )
                return False
            time.sleep(min(self.vacuum_idle_poll_s, remaining_s))

    def _record_compaction_held(self, *, reason: str, sizes: dict[str, int]) -> None:
        """Record a rebuild held back by an active bridge task.

        No waiting happens here: the hold is released by the bridge when the last
        task finishes, and that release re-schedules the rebuild.
        """
        database_bytes = int(sizes.get("databaseBytes") or 0)
        status = {
            "status": MAINTENANCE_STATUS_HELD_ACTIVE_TASK,
            "reason": str(reason or ""),
            "durationMs": 0,
            "reclaimedBytes": 0,
            "error": "",
            "effectiveFreelistRatio": self.effective_vacuum_freelist_ratio(database_bytes),
            "freelistBytes": int(sizes.get("freelistBytes") or 0),
            "databaseBytes": database_bytes,
        }
        with self._maintenance_lock:
            self._last_vacuum = status
        LOGGER.info("SQLite VACUUM held while a bridge task is active: %s", reason)

    def _record_compaction_skipped(
        self,
        *,
        reason: str,
        sizes: dict[str, int],
        waited_s: float,
        idle_for_s: float,
        idle_gate_s: float,
    ) -> None:
        database_bytes = int(sizes.get("databaseBytes") or 0)
        status = {
            "status": MAINTENANCE_STATUS_SKIPPED_BUSY,
            "reason": str(reason or ""),
            "durationMs": int(round(waited_s * 1000)),
            "reclaimedBytes": 0,
            "error": "",
            "idleForS": round(idle_for_s, 3),
            "effectiveIdleGateS": idle_gate_s,
            "idleGateS": self.vacuum_idle_s,
            "effectiveFreelistRatio": self.effective_vacuum_freelist_ratio(database_bytes),
            "freelistBytes": int(sizes.get("freelistBytes") or 0),
            "databaseBytes": database_bytes,
        }
        with self._maintenance_lock:
            self._last_vacuum = status
        LOGGER.info(
            "SQLite VACUUM skipped while the store is busy (idle %.1fs of %.1fs): %s",
            idle_for_s,
            idle_gate_s,
            reason,
        )

    def backup_to(self, target_path: Path | str) -> Path:
        quick_check = self.quick_check()
        if quick_check != "ok":
            self._healthy = False
            raise BaluffoStoreError(f"SQLite quick_check failed before backup: {quick_check}")
        self.checkpoint_required(mode="TRUNCATE")
        target = Path(target_path).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        destination = sqlite3.connect(str(target), isolation_level=None)
        try:
            with self._write_lock:
                self._writer.backup(destination)
        finally:
            destination.close()
        self._validate_database_file(target)
        return target

    @classmethod
    def restore_backup(
        cls,
        backup_path: Path | str,
        data_dir: Path | str,
        *,
        db_name: str = DEFAULT_DB_NAME,
        **kwargs: Any,
    ) -> BaluffoStore:
        source_path = Path(backup_path).expanduser().resolve()
        if not source_path.is_file():
            raise BaluffoStoreError(f"SQLite backup file not found: {source_path}")
        target_dir = Path(data_dir).expanduser().resolve()
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / db_name
        source = sqlite3.connect(str(source_path), isolation_level=None)
        destination = sqlite3.connect(str(target_path), isolation_level=None)
        try:
            source.backup(destination)
        finally:
            destination.close()
            source.close()
        cls._validate_database_file(target_path)
        return cls(target_dir, db_name=db_name, **kwargs)

    @staticmethod
    def _validate_database_file(path: Path) -> None:
        conn = sqlite3.connect(str(path), isolation_level=None)
        try:
            result = str(conn.execute("PRAGMA quick_check").fetchone()[0] or "")
        finally:
            conn.close()
        if result != "ok":
            raise BaluffoStoreError(f"SQLite quick_check failed for {path}: {result}")

    def health(self) -> dict[str, Any]:
        quick_check, quick_check_deferred = self.quick_check_for_health()
        migration_version = self.migration_version()
        busy_rate = self._busy_count / max(1, self._write_attempts)
        sizes = self.file_sizes()
        quick_check_ok = quick_check == "ok" or quick_check_deferred
        return {
            "schemaVersion": STORAGE_SCHEMA_VERSION,
            "databasePath": str(self.db_path),
            **sizes,
            "migrationVersion": migration_version,
            "walMode": self.wal_mode(),
            "foreignKeys": self._foreign_keys_enabled(),
            "quickCheck": quick_check,
            "quickCheckScope": "deferred" if quick_check_deferred else "limited",
            "quickCheckDeferred": quick_check_deferred,
            "quickCheckMaxDatabaseBytes": self.quick_check_max_database_bytes,
            "healthy": bool(self._healthy and quick_check_ok),
            "lastWriteError": self._last_write_error,
            "busyCount": self._busy_count,
            "busyRate": busy_rate,
            "writeAttempts": self._write_attempts,
            "writeTransactions": self._write_transactions,
            "walMaintenance": self.wal_maintenance_status(),
            "compaction": self.compaction_status(),
            "authorityModes": self.get_authority_modes(),
        }

    def _foreign_keys_enabled(self) -> int:
        with self._write_lock:
            return int(self._writer.execute("PRAGMA foreign_keys").fetchone()[0] or 0)
