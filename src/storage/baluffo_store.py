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
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from typing import Any, TypeVar

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
        self._maintenance_thread: threading.Thread | None = None
        self._write_lock = threading.RLock()
        self._read_lock = threading.RLock()
        self._maintenance_lock = threading.RLock()
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
            self._write_attempts += 1
            try:
                with self._write_lock:
                    self._writer.execute("BEGIN IMMEDIATE")
                    result = callback(self._writer)
                    self._writer.commit()
                self._last_write_error = ""
                self._write_transactions += 1
                self.schedule_wal_maintenance_if_needed(reason="write")
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
        return conn.execute(sql, parameters).fetchone()

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
        with self._write_lock:
            return self._writer.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()

    @staticmethod
    def _path_size(path: Path) -> int:
        try:
            return max(0, int(path.stat().st_size))
        except OSError:
            return 0

    def file_sizes(self) -> dict[str, int]:
        return {
            "databaseBytes": self._path_size(self.db_path),
            "walBytes": self._path_size(self.db_path.with_name(f"{self.db_path.name}-wal")),
            "shmBytes": self._path_size(self.db_path.with_name(f"{self.db_path.name}-shm")),
        }

    def wal_maintenance_status(self) -> dict[str, Any]:
        with self._maintenance_lock:
            running = self._maintenance_thread is not None and self._maintenance_thread.is_alive()
            return {
                **self._last_checkpoint,
                "running": running,
                "checkpointThresholdBytes": self.wal_checkpoint_threshold_bytes,
                "truncateThresholdBytes": self.wal_truncate_threshold_bytes,
            }

    def schedule_wal_maintenance_if_needed(self, *, reason: str = "") -> bool:
        sizes = self.file_sizes()
        wal_bytes = int(sizes.get("walBytes") or 0)
        if wal_bytes < self.wal_checkpoint_threshold_bytes:
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
            "authorityModes": self.get_authority_modes(),
        }

    def _foreign_keys_enabled(self) -> int:
        with self._write_lock:
            return int(self._writer.execute("PRAGMA foreign_keys").fetchone()[0] or 0)
