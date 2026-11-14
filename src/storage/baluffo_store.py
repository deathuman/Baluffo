"""SQLite/WAL runtime storage skeleton for bridge-owned hot state.

AI boundary owns: SQLite connection, migration, WAL, transaction, and batch helpers for runtime storage.
AI boundary implement in: this file for store primitives; domain row APIs stay in sibling storage modules.
AI boundary search before contracts: storage domain modules, migrations, and runtime storage tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused storage runtime tests.
"""

from __future__ import annotations

import logging
import os
import shutil
import sqlite3
import threading
import time
import traceback
import uuid
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar, cast

from src.storage.retention_cancel import RetentionCancelled
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
# Generation cleanup deletes a very large number of rows. Its per-batch writes
# must not trigger the normal 64 MiB background checkpoint: that turns a bounded
# delete loop into repeated write-lock stalls. One larger auto-checkpoint keeps
# the WAL bounded while the bulk delete runs, and the context checkpoints once at
# the end before restoring the normal writer settings.
DEFAULT_BULK_DELETE_WAL_AUTOCHECKPOINT_PAGES = 20_000
BULK_DELETE_SECURE_DELETE = "FAST"
# A passive checkpoint can leave the WAL file at the journal size limit: SQLite
# has checkpointed the frames, but the file still looks over the threshold. A
# new write must not turn that into a checkpoint loop, so background scheduling
# is rate-limited independently of the size check.
DEFAULT_WAL_CHECKPOINT_COOLDOWN_S = 60.0
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
# Retention is a maintenance cycle, not a one-shot startup task. A long-running
# container must keep pruning old source payloads as new fetch runs arrive.
DEFAULT_RUNTIME_RETENTION_INTERVAL_S = 6 * 60 * 60
# A bounded pass may leave more work than its cap. Continue soon while the
# backlog is being drained, then fall back to the normal interval.
DEFAULT_RUNTIME_RETENTION_FOLLOWUP_S = 30.0
DEFAULT_RUNTIME_RETENTION_RETRY_S = 300.0
# One SQLite write transaction should take tens of milliseconds. A write that
# waits longer than this to take the store write lock, or holds it longer, is
# recorded with its thread and stack: that is the only evidence that attributes a
# multi-second startup or route stall to the writer that caused it.
DEFAULT_SLOW_WRITE_WARN_MS = 1000
# `busy_timeout` bounds SQLite's own contention, not the in-process write lock.
# Maintenance holds that lock for a whole VACUUM (tens of seconds, minutes on a
# multi-gigabyte database), so the wait must be generous but still bounded: a
# wedged holder would otherwise park every later writer forever with no error,
# no journal row, and a task that never leaves `running`.
DEFAULT_WRITE_LOCK_TIMEOUT_MS = 300_000
# Diagnostics must not wait on that lock at all: maintenance holds it for a whole
# VACUUM, and /ops/storage-health answering late is what turned a busy store into
# a gateway timeout and then into a "bridge offline" badge. The read falls back to
# the last value taken while the writer was idle, and says so.
DEFAULT_HEALTH_LOCK_TIMEOUT_MS = 250
# Per-table space estimates come from a bounded sample, so they are cached:
# `/ops/storage-health` is polled on a timer and must not re-sample every poll.
DEFAULT_TABLE_SIZE_CACHE_S = 600.0
# A partial sample is useful immediately but must not freeze the panel for the
# full cache window: retry it on the next health poll instead.
DEFAULT_TABLE_SIZE_PARTIAL_CACHE_S = 5.0
# An in-place VACUUM holds the store write lock for the whole rebuild, which on a
# multi-gigabyte runtime database is an unbounded hold -- the same shape that
# starved fetch finalization behind the jobs-feed reconciliation lock. A
# post-retention shrink therefore rewrites the database into a compact copy with
# VACUUM INTO while writers keep running, then swaps it in under both locks. The
# run is bounded by a wall-clock budget and by free disk space, and the original
# file stays in place until the copy validates.
DEFAULT_COMPACTION_BUDGET_S = 900.0
DEFAULT_COMPACTION_FREE_SPACE_MARGIN_BYTES = 64 * 1024 * 1024
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


class _WriteLockTimeout(BaluffoStoreError):
    """The store write lock was never acquired; no transaction was opened."""


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
        wal_checkpoint_cooldown_s: float = DEFAULT_WAL_CHECKPOINT_COOLDOWN_S,
        quick_check_max_database_bytes: int = DEFAULT_QUICK_CHECK_MAX_DATABASE_BYTES,
        vacuum_freelist_threshold_bytes: int = DEFAULT_VACUUM_FREELIST_THRESHOLD_BYTES,
        vacuum_freelist_ratio: float = DEFAULT_VACUUM_FREELIST_RATIO,
        vacuum_large_database_bytes: int = DEFAULT_VACUUM_LARGE_DATABASE_BYTES,
        vacuum_large_database_ratio: float = DEFAULT_VACUUM_LARGE_DATABASE_RATIO,
        vacuum_idle_s: float = DEFAULT_VACUUM_IDLE_S,
        vacuum_large_database_idle_s: float = DEFAULT_VACUUM_LARGE_DATABASE_IDLE_S,
        vacuum_idle_max_wait_s: float = DEFAULT_VACUUM_IDLE_MAX_WAIT_S,
        vacuum_idle_poll_s: float = DEFAULT_VACUUM_IDLE_POLL_S,
        compaction_budget_s: float = DEFAULT_COMPACTION_BUDGET_S,
        defer_maintenance: bool = False,
        compaction_hold: bool = False,
        slow_write_warn_ms: int = DEFAULT_SLOW_WRITE_WARN_MS,
        write_lock_timeout_ms: int = DEFAULT_WRITE_LOCK_TIMEOUT_MS,
        health_lock_timeout_ms: int = DEFAULT_HEALTH_LOCK_TIMEOUT_MS,
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
        self.wal_checkpoint_cooldown_s = max(0.0, float(wal_checkpoint_cooldown_s))
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
        self.compaction_budget_s = max(0.0, float(compaction_budget_s))
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
        self._passive_checkpoint_count = 0
        self._passive_checkpoint_wait_ms_total = 0
        self._passive_checkpoint_hold_ms_total = 0
        self._passive_checkpoint_wait_ms_max = 0
        self._passive_checkpoint_hold_ms_max = 0
        self._passive_checkpoint_wait_ms_last = 0
        self._passive_checkpoint_hold_ms_last = 0
        self._passive_checkpoint_busy_retries = 0
        self._last_vacuum: dict[str, Any] = {
            "status": "not-run",
            "reason": "",
            "durationMs": 0,
            "reclaimedBytes": 0,
            "error": "",
        }
        self._last_retention: dict[str, Any] = {
            "status": "not-run",
            "phase": "not-run",
            "stage": "",
            "reason": "",
            "startedAt": "",
            "completedAt": "",
            "updatedAt": "",
            "durationMs": 0,
            "elapsedMs": 0,
            "deletedRunCount": 0,
            "deletedRowCount": 0,
            "deletedEventCount": 0,
            "deletedSyncRunCount": 0,
            "deletedGenerationCount": 0,
            "processedGenerationCount": 0,
            "totalGenerationCount": 0,
            "totalGenerationRowCount": 0,
            "deletedGenerationRowCount": 0,
            "generationRowCountKnown": False,
            "candidateRunCount": 0,
            "processedRunCount": 0,
            "totalRunCount": 0,
            "jobGenerations": {},
            "truncated": False,
            "error": "",
        }
        self._retention_started_monotonic = 0.0
        self._retention_in_progress = False
        self._retention_starting = False
        self._runtime_retention_done = False
        self._retention_thread: threading.Thread | None = None
        self._retention_run_id = ""
        self._retention_cancel = threading.Event()
        self._retention_cancelled_last_run = False
        self._retention_timer: threading.Timer | None = None
        self._retention_next_run_monotonic = 0.0
        self._closed = False
        self._bulk_delete_mode = False
        self._table_sizes_lock = threading.Lock()
        self._table_sizes_snapshot: dict[str, Any] | None = None
        self._table_sizes_cache_until_monotonic = 0.0
        self._maintenance_thread: threading.Thread | None = None
        self._compaction_in_progress = False
        self._wal_maintenance_cooldown_until_monotonic = 0.0
        self._write_lock = threading.RLock()
        self._write_lock_state_lock = threading.Lock()
        self._write_lock_owner_ident: int | None = None
        self._write_lock_owner_name = ""
        self._write_lock_owner_native_id: int | None = None
        self._write_lock_acquired_monotonic = 0.0
        self._write_lock_acquired_at = ""
        self._write_lock_operation = ""
        self._write_lock_depth = 0
        self._read_lock = threading.RLock()
        self._maintenance_lock = threading.RLock()
        self._maintenance_deferred = bool(defer_maintenance)
        self._compaction_hold = bool(compaction_hold)
        self.slow_write_warn_ms = max(0, int(slow_write_warn_ms))
        self.write_lock_timeout_ms = max(1, int(write_lock_timeout_ms))
        self.health_lock_timeout_ms = max(1, int(health_lock_timeout_ms))
        # Captured while the writer is idle at open; health() reports these instead
        # of queueing behind a held write lock.
        self._last_known_wal_mode = "wal"
        self._last_known_foreign_keys = 1
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
        with self._maintenance_lock:
            self._closed = True
            timer = self._retention_timer
            self._retention_timer = None
            self._retention_next_run_monotonic = 0.0
            retention_thread = self._retention_thread
            maintenance_thread = self._maintenance_thread
        if timer is not None:
            timer.cancel()
        for thread in (retention_thread, maintenance_thread):
            if thread is not None and thread is not threading.current_thread():
                # Retention, WAL, and compaction passes all hold a SQLite
                # connection. Closing underneath one is a use-after-close, so
                # wait for the bounded pass before releasing the database.
                thread.join()
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
        self._last_known_wal_mode = wal_mode
        self._writer.execute("PRAGMA synchronous=NORMAL")
        self._writer.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        self._writer.execute("PRAGMA foreign_keys=ON")
        self._writer.execute("PRAGMA wal_autocheckpoint=1000")
        self._writer.execute(f"PRAGMA journal_size_limit={DEFAULT_WAL_JOURNAL_SIZE_LIMIT_BYTES}")
        foreign_keys = int(self._writer.execute("PRAGMA foreign_keys").fetchone()[0] or 0)
        if foreign_keys != 1:
            self._healthy = False
            raise BaluffoStoreError("Unable to enable SQLite foreign key enforcement")
        self._last_known_foreign_keys = foreign_keys

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

    def write(
        self,
        callback: Callable[[sqlite3.Connection], _T],
        *,
        secure_delete: str | None = None,
        operation: str = "write",
    ) -> _T:
        operation_label = str(operation or "write").strip() or "write"
        secure_delete_mode = str(secure_delete or "").upper() or None
        if secure_delete_mode not in {None, "ON", "OFF", "FAST"}:
            raise ValueError(f"Unsupported SQLite secure_delete mode: {secure_delete}")
        last_busy_error: sqlite3.OperationalError | None = None
        for attempt in range(self.busy_retry_attempts):
            # Every attempt, including a busy retry, counts as write activity: the
            # idle gate must not start a rebuild while writers are still arriving.
            self._last_write_monotonic = time.monotonic()
            self._write_attempts += 1
            try:
                lock_started = time.perf_counter()
                if not self._acquire_write_lock(
                    timeout_s=self.write_lock_timeout_ms / 1000.0,
                    operation="write",
                ):
                    waited_s = time.perf_counter() - lock_started
                    self._record_slow_write(
                        operation="write_lock_timeout", waited_s=waited_s, held_s=0.0
                    )
                    self._healthy = False
                    self._last_write_error = "store write lock still held"
                    raise _WriteLockTimeout(
                        f"store write lock still held after {int(waited_s)}s "
                        f"(write_lock_timeout_ms={self.write_lock_timeout_ms})"
                    )
                try:
                    waited_s = time.perf_counter() - lock_started
                    result, held_s, timings = self._write_transaction(callback, secure_delete_mode)
                finally:
                    self._release_write_lock()
                self._record_slow_write(
                    operation=operation_label,
                    waited_s=waited_s,
                    held_s=held_s,
                    timings=timings,
                )
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
                if isinstance(exc, _WriteLockTimeout):
                    # The lock was never acquired, so nothing was begun on the shared
                    # writer connection: rolling back here would abort the holder's
                    # in-flight transaction instead.
                    LOGGER.error(str(exc))
                    raise
                self._rollback_after_error()
                self._healthy = False
                self._last_write_error = str(exc)
                raise
        message = f"SQLite write failed after busy retry attempts: {last_busy_error}"
        LOGGER.error(message)
        raise BaluffoStoreError(message)

    def _write_transaction(
        self,
        callback: Callable[[sqlite3.Connection], _T],
        secure_delete_mode: str | None,
    ) -> tuple[_T, float, dict[str, int]]:
        """Run one write transaction, optionally with a transaction-scoped pragma."""
        secure_delete_before: int | None = None
        wal_path = self.db_path.with_name(f"{self.db_path.name}-wal")
        hold_started = time.perf_counter()
        try:
            if secure_delete_mode is not None:
                secure_delete_before = int(
                    self._writer.execute("PRAGMA secure_delete").fetchone()[0] or 0
                )
                self._writer.execute(f"PRAGMA secure_delete={secure_delete_mode}")
            self._writer.execute("BEGIN IMMEDIATE")
            wal_bytes_before = self._path_size(wal_path)
            callback_started = time.perf_counter()
            try:
                result = callback(self._writer)
                callback_ms = int(round((time.perf_counter() - callback_started) * 1000))
                commit_started = time.perf_counter()
                self._writer.commit()
                commit_ms = int(round((time.perf_counter() - commit_started) * 1000))
            except BaseException:
                self._writer.rollback()
                raise
            wal_bytes_after = self._path_size(wal_path)
            timings = {
                "callbackMs": max(0, callback_ms),
                "commitMs": max(0, commit_ms),
                "walBytesBefore": wal_bytes_before,
                "walBytesAfter": wal_bytes_after,
                "walGrowthBytes": wal_bytes_after - wal_bytes_before,
            }
            return result, time.perf_counter() - hold_started, timings
        finally:
            if secure_delete_before is not None:
                self._writer.execute(f"PRAGMA secure_delete={secure_delete_before}")

    @contextmanager
    def bulk_delete_mode(self) -> Iterator[None]:
        """Use cleanup-friendly SQLite settings for a bounded bulk delete.

        A 250-row generation-cleanup batch used to trigger SQLite's 1,000-page
        auto-checkpoint (and, once the WAL crossed 64 MiB, the store's own
        background checkpoint). On the measured 108 MiB subset that churn held
        the write lock for 16-20 s and capped throughput near 3,900 rows/min.
        Keep the WAL bounded, but checkpoint at 20,000 pages instead, suppress
        the duplicate background scheduling, and checkpoint once on exit.

        This context changes only WAL/checkpoint policy. Individual cleanup
        delete transactions request `secure_delete=FAST` themselves, so unrelated
        runtime writes keep the writer's normal secure-delete setting. Foreign-key
        enforcement and NORMAL synchronous durability stay enabled. The final
        checkpoint uses TRUNCATE: a PASSIVE checkpoint leaves the WAL at the 64 MiB
        journal-size limit, which makes the next write schedule another background
        checkpoint.
        """
        if self._bulk_delete_mode:
            yield
            return
        with self._write_lock_guard("bulk-delete-mode"):
            auto_checkpoint = int(
                self._writer.execute("PRAGMA wal_autocheckpoint").fetchone()[0] or 0
            )
            self._writer.execute(
                f"PRAGMA wal_autocheckpoint={DEFAULT_BULK_DELETE_WAL_AUTOCHECKPOINT_PAGES}"
            )
            self._bulk_delete_mode = True
        try:
            yield
        finally:
            with self._write_lock_guard("bulk-delete-mode-restore"):
                self._bulk_delete_mode = False
                self._writer.execute(f"PRAGMA wal_autocheckpoint={auto_checkpoint}")
            try:
                self.checkpoint_required(mode="TRUNCATE")
            except (BaluffoStoreError, sqlite3.Error) as exc:
                LOGGER.warning("SQLite bulk-delete final checkpoint failed: %s", exc)

    def _record_slow_write(
        self,
        *,
        operation: str,
        waited_s: float,
        held_s: float,
        timings: dict[str, int] | None = None,
    ) -> None:
        """Journal a write-lock operation that will not go unnoticed in the field.

        A waiter and a holder both write a row, so the stalled caller and the
        writer that blocked it are attributable from the same journal. The
        optional per-transaction timings separate statement work from commit and
        checkpoint cost.
        """
        if self.slow_write_warn_ms <= 0:
            return
        wait_ms = int(round(max(0.0, waited_s) * 1000))
        hold_ms = int(round(max(0.0, held_s) * 1000))
        if wait_ms < self.slow_write_warn_ms and hold_ms < self.slow_write_warn_ms:
            return
        timing_payload = dict(timings or {})
        if operation.startswith("cleanup-delete:"):
            timing_payload["deleteMs"] = max(0, int(timing_payload.get("callbackMs") or 0))
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
                timings=timing_payload,
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            # Diagnostics must never break a committed write.
            return

    def _acquire_write_lock(self, *, timeout_s: float | None, operation: str) -> bool:
        if timeout_s is None:
            acquired = self._write_lock.acquire()
        else:
            acquired = self._write_lock.acquire(timeout=max(0.0, float(timeout_s)))
        if acquired:
            ident = threading.get_ident()
            thread = threading.current_thread()
            with self._write_lock_state_lock:
                if self._write_lock_owner_ident == ident:
                    self._write_lock_depth += 1
                else:
                    self._write_lock_owner_ident = ident
                    self._write_lock_owner_name = thread.name
                    self._write_lock_owner_native_id = getattr(thread, "native_id", None)
                    self._write_lock_acquired_monotonic = time.monotonic()
                    self._write_lock_acquired_at = _now_iso()
                    self._write_lock_operation = str(operation or "write")
                    self._write_lock_depth = 1
        return bool(acquired)

    def _release_write_lock(self) -> None:
        ident = threading.get_ident()
        with self._write_lock_state_lock:
            if self._write_lock_owner_ident == ident:
                self._write_lock_depth = max(0, self._write_lock_depth - 1)
                if self._write_lock_depth == 0:
                    self._write_lock_owner_ident = None
                    self._write_lock_owner_name = ""
                    self._write_lock_owner_native_id = None
                    self._write_lock_acquired_monotonic = 0.0
                    self._write_lock_acquired_at = ""
                    self._write_lock_operation = ""
            self._write_lock.release()

    @contextmanager
    def _write_lock_guard(self, operation: str) -> Iterator[None]:
        if not self._acquire_write_lock(timeout_s=None, operation=operation):
            raise RuntimeError("could not acquire the SQLite write lock")
        try:
            yield
        finally:
            self._release_write_lock()

    def write_lock_status(self) -> dict[str, Any]:
        """Return the current in-process SQLite write-lock holder."""
        with self._write_lock_state_lock:
            held = self._write_lock_owner_ident is not None
            held_ms = 0
            if held:
                held_ms = max(
                    0,
                    int(round((time.monotonic() - self._write_lock_acquired_monotonic) * 1000)),
                )
            return {
                "held": held,
                "threadName": self._write_lock_owner_name,
                "threadId": self._write_lock_owner_ident,
                "threadNativeId": self._write_lock_owner_native_id,
                "operation": self._write_lock_operation,
                "depth": self._write_lock_depth,
                "acquiredAt": self._write_lock_acquired_at,
                "heldMs": held_ms,
            }

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

    def execute_read_with_deadline(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
        *,
        deadline: float,
    ) -> list[dict[str, Any]] | None:
        """Read all rows, aborting SQLite work at a monotonic deadline.

        Returns ``None`` when SQLite interrupted the statement because the
        deadline expired. Other database errors still propagate so callers can
        distinguish a bounded miss from a broken read.
        """

        def read(conn: sqlite3.Connection) -> list[sqlite3.Row] | None:
            conn.set_progress_handler(
                lambda: 1 if time.monotonic() >= deadline else 0,
                1000,
            )
            try:
                return self._execute_read_fetchall(conn, sql, parameters)
            except sqlite3.OperationalError as exc:
                if "interrupted" in str(exc).lower():
                    return None
                raise
            finally:
                conn.set_progress_handler(None, 0)

        rows = self._read_with_retry(read)
        return None if rows is None else [dict(row) for row in rows]

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

    def _pragma_with_source(self, sql: str, fallback: Any) -> tuple[Any, str]:
        """Read a writer-only pragma without waiting on a held write lock.

        A VACUUM can hold the write lock for tens of seconds, and minutes on a
        multi-gigabyte database. Diagnostics used to queue behind it and the route
        never answered at all, so the caller gets the value captured while the
        writer was last idle, tagged `cached-busy`, instead of an unbounded wait.
        """
        if not self._acquire_write_lock(
            timeout_s=self.health_lock_timeout_ms / 1000.0,
            operation="health-pragma",
        ):
            return fallback, "cached-busy"
        try:
            return self._writer.execute(sql).fetchone()[0], "live"
        finally:
            self._release_write_lock()

    def wal_mode(self) -> str:
        wal_mode, _source = self._pragma_with_source(
            "PRAGMA journal_mode", self._last_known_wal_mode
        )
        return str(wal_mode or "").lower()

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
                self._record_passive_checkpoint_busy_retry(checkpoint_mode)
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
                self._record_passive_checkpoint_busy_retry(checkpoint_mode)
                self._sleep_for_retry(attempt)
        message = f"SQLite WAL checkpoint failed after busy retry attempts: {last_result}"
        LOGGER.error(message)
        raise BaluffoStoreError(message)

    def _execute_checkpoint(self, mode: str) -> sqlite3.Row:
        lock_started = time.perf_counter()
        wal_path = self.db_path.with_name(f"{self.db_path.name}-wal")
        with self._write_lock_guard(f"checkpoint:{str(mode or '').lower()}"):
            waited_s = time.perf_counter() - lock_started
            wal_bytes_before = self._path_size(wal_path)
            hold_started = time.perf_counter()
            row = cast(
                sqlite3.Row, self._writer.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
            )
            held_s = time.perf_counter() - hold_started
            wal_bytes_after = self._path_size(wal_path)
        self._record_passive_checkpoint_timing(
            mode=mode,
            waited_s=waited_s,
            held_s=held_s,
            busy=int(row[0] or 0),
        )
        self._record_slow_write(
            operation=f"checkpoint:{str(mode or '').lower()}",
            waited_s=waited_s,
            held_s=held_s,
            timings={
                "checkpointMs": max(0, int(round(held_s * 1000))),
                "walBytesBefore": wal_bytes_before,
                "walBytesAfter": wal_bytes_after,
                "walGrowthBytes": wal_bytes_after - wal_bytes_before,
            },
        )
        return row

    def _record_passive_checkpoint_busy_retry(self, mode: str) -> None:
        if str(mode or "").upper() != "PASSIVE":
            return
        with self._maintenance_lock:
            self._passive_checkpoint_busy_retries += 1

    def _record_passive_checkpoint_timing(
        self, *, mode: str, waited_s: float, held_s: float, busy: int
    ) -> None:
        # Only completed PASSIVE checkpoints belong in these counters. A busy
        # result is a retryable attempt, not completed checkpoint work.
        if str(mode or "").upper() != "PASSIVE" or int(busy) != 0:
            return
        wait_ms = max(0, int(round(waited_s * 1000)))
        hold_ms = max(0, int(round(held_s * 1000)))
        with self._maintenance_lock:
            self._passive_checkpoint_count += 1
            self._passive_checkpoint_wait_ms_total += wait_ms
            self._passive_checkpoint_hold_ms_total += hold_ms
            self._passive_checkpoint_wait_ms_max = max(
                self._passive_checkpoint_wait_ms_max, wait_ms
            )
            self._passive_checkpoint_hold_ms_max = max(
                self._passive_checkpoint_hold_ms_max, hold_ms
            )
            self._passive_checkpoint_wait_ms_last = wait_ms
            self._passive_checkpoint_hold_ms_last = hold_ms

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
                "cooldownS": self.wal_checkpoint_cooldown_s,
                "cooldownRemainingMs": self._wal_checkpoint_cooldown_remaining_ms_locked(
                    time.monotonic()
                ),
                "passiveCheckpoint": {
                    "count": self._passive_checkpoint_count,
                    "waitMsTotal": self._passive_checkpoint_wait_ms_total,
                    "holdMsTotal": self._passive_checkpoint_hold_ms_total,
                    "waitMsMax": self._passive_checkpoint_wait_ms_max,
                    "holdMsMax": self._passive_checkpoint_hold_ms_max,
                    "waitMsLast": self._passive_checkpoint_wait_ms_last,
                    "holdMsLast": self._passive_checkpoint_hold_ms_last,
                },
                "passiveCheckpointBusyRetries": self._passive_checkpoint_busy_retries,
            }

    def _wal_checkpoint_cooldown_remaining_ms_locked(self, now: float) -> int:
        if self.wal_checkpoint_cooldown_s <= 0.0:
            return 0
        return max(
            0,
            int(round((self._wal_maintenance_cooldown_until_monotonic - now) * 1000)),
        )

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
        self.schedule_runtime_retention(reason="compaction-hold-released", force=True)
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
            if self._closed:
                return False
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
            self._run_runtime_retention(reason=reason)
            with self._maintenance_lock:
                if self._retention_cancelled_last_run:
                    # An operator stopped this pass. Do not turn the stop into a
                    # compaction/WAL run that would hold the write lock anyway.
                    return
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

    def _set_retention_held(self, reason: str) -> None:
        self._last_retention = {
            "status": MAINTENANCE_STATUS_HELD_ACTIVE_TASK,
            "phase": MAINTENANCE_STATUS_HELD_ACTIVE_TASK,
            "stage": "held",
            "reason": str(reason or ""),
            "startedAt": "",
            "completedAt": "",
            "updatedAt": _now_iso(),
            "durationMs": 0,
            "elapsedMs": 0,
            "deletedRunCount": 0,
            "deletedRowCount": 0,
            "deletedEventCount": 0,
            "deletedSyncRunCount": 0,
            "deletedGenerationCount": 0,
            "processedGenerationCount": 0,
            "totalGenerationCount": 0,
            "totalGenerationRowCount": 0,
            "deletedGenerationRowCount": 0,
            "generationRowCountKnown": False,
            "candidateRunCount": 0,
            "processedRunCount": 0,
            "totalRunCount": 0,
            "error": "",
        }

    def runtime_retention_status(self) -> dict[str, Any]:
        with self._maintenance_lock:
            next_run_in_s = 0.0
            if self._retention_timer is not None:
                next_run_in_s = max(0.0, self._retention_next_run_monotonic - time.monotonic())
            running = bool(self._retention_in_progress)
            starting = bool(self._retention_starting)
            elapsed_ms = int(self._last_retention.get("elapsedMs") or 0)
            if running and self._retention_started_monotonic:
                elapsed_ms = max(
                    elapsed_ms,
                    int(round((time.monotonic() - self._retention_started_monotonic) * 1000)),
                )
            last_status = str(self._last_retention.get("status") or "")
            follow_up_scheduled = False
            retry_scheduled = False
            needs_more = bool(self._last_retention.get("truncated")) and not (
                self._runtime_retention_done
            )
            if not running and not starting and self._retention_timer is not None:
                if last_status == "failed" and not self._runtime_retention_done:
                    retry_scheduled = True
                elif not self._runtime_retention_done and last_status not in {
                    "cancelled",
                    MAINTENANCE_STATUS_HELD_ACTIVE_TASK,
                }:
                    follow_up_scheduled = True
            elif (
                not running
                and not starting
                and needs_more
                and last_status not in {"failed", "cancelled", MAINTENANCE_STATUS_HELD_ACTIVE_TASK}
            ):
                # The worker has published its finished status but has not
                # attached the follow-up timer yet. Never expose that narrow
                # window as a fully complete pass.
                follow_up_scheduled = True
            if running or starting:
                completion_state = "running"
            elif last_status == "cancelled":
                completion_state = "cancelled"
            elif retry_scheduled:
                completion_state = "retry-scheduled"
            elif last_status == "failed":
                completion_state = "failed"
            elif last_status == MAINTENANCE_STATUS_HELD_ACTIVE_TASK:
                completion_state = "held-active-task"
            elif follow_up_scheduled:
                completion_state = "follow-up-scheduled"
            elif self._runtime_retention_done:
                completion_state = "complete"
            else:
                completion_state = last_status or "not-run"
            status = {
                **self._last_retention,
                "runId": str(self._retention_run_id or ""),
                "done": bool(self._runtime_retention_done),
                "running": running or starting,
                "starting": starting,
                "cancelRequested": bool(self._retention_cancel.is_set()),
                "timerScheduled": bool(self._retention_timer is not None),
                "nextRunInS": round(next_run_in_s, 1),
                "followUpScheduled": follow_up_scheduled,
                "followUpInS": round(next_run_in_s, 1) if follow_up_scheduled else 0.0,
                "retryScheduled": retry_scheduled,
                "retryInS": round(next_run_in_s, 1) if retry_scheduled else 0.0,
                "fullyComplete": bool(
                    self._runtime_retention_done
                    and not running
                    and not starting
                    and last_status
                    not in {"cancelled", "failed", MAINTENANCE_STATUS_HELD_ACTIVE_TASK}
                ),
                "completionState": completion_state,
                "elapsedMs": elapsed_ms,
            }
            if starting and status.get("status") != "cancelling":
                # The scheduler has accepted a pass but its worker has not
                # published its first phase yet. Do not surface a stale
                # previous result as the current state.
                status.update({"status": "running", "phase": "starting", "stage": "starting"})
            return status

    def cancel_runtime_retention(self, run_id: str = "") -> dict[str, Any]:
        """Request cancellation of one specific retention run.

        The running batch is never interrupted mid-transaction: its commit,
        bulk-delete pragmas, and WAL restoration finish normally. The next
        batch or phase observes this flag and ends the pass. A missing or stale
        run id is rejected rather than being applied to a newer pass.
        """
        with self._maintenance_lock:
            active = bool(self._retention_in_progress or self._retention_starting)
            requested_run_id = str(run_id or "").strip()
            current_run_id = str(self._retention_run_id or "")
            if not active:
                return {
                    "requested": False,
                    "cancelling": False,
                    "stale": bool(requested_run_id),
                    "reason": "no-running-pass",
                    "runtimeRetention": self.runtime_retention_status(),
                }
            if not requested_run_id:
                return {
                    "requested": False,
                    "cancelling": False,
                    "stale": True,
                    "reason": "run-id-required",
                    "runtimeRetention": self.runtime_retention_status(),
                }
            if requested_run_id != current_run_id:
                return {
                    "requested": False,
                    "cancelling": False,
                    "stale": True,
                    "reason": "run-id-mismatch",
                    "runtimeRetention": self.runtime_retention_status(),
                }
            self._retention_cancel.set()
            self._last_retention = {
                **self._last_retention,
                "status": "cancelling",
                "phase": "cancelling",
                "stage": "cancelling",
                "updatedAt": _now_iso(),
            }
            return {
                "requested": True,
                "cancelling": True,
                "stale": False,
                "reason": "",
                "runtimeRetention": self.runtime_retention_status(),
            }

    def schedule_runtime_retention(self, *, reason: str = "", force: bool = False) -> bool:
        """Start one bounded runtime-retention pass in the background.

        Retention deletes old source-run payloads and event history. It is
        scheduled after serving and after the compaction hold is released, so
        it never competes with an active fetch on the startup path.
        """
        with self._maintenance_lock:
            if self._closed:
                return False
            if self._runtime_retention_done and not force:
                return False
            if self._retention_starting or (
                self._retention_thread is not None and self._retention_thread.is_alive()
            ):
                return False
            if self._retention_timer is not None:
                self._retention_timer.cancel()
                self._retention_timer = None
                self._retention_next_run_monotonic = 0.0
            if self._compaction_hold:
                self._set_retention_held(reason)
                return False
            if force:
                self._runtime_retention_done = False
            self._retention_cancel.clear()
            self._retention_run_id = f"retention_{uuid.uuid4().hex[:12]}"
            self._retention_starting = True
            thread = threading.Thread(
                target=self._run_runtime_retention,
                kwargs={"reason": str(reason or "")},
                name="baluffo-sqlite-runtime-retention",
                daemon=True,
            )
            self._retention_thread = thread
            try:
                thread.start()
            except BaseException:
                self._retention_starting = False
                self._retention_thread = None
                self._retention_run_id = ""
                raise
            return True

    def _run_runtime_retention(self, *, reason: str) -> bool:
        started = time.monotonic()
        started_at = _now_iso()
        with self._maintenance_lock:
            scheduled = self._retention_starting
            self._retention_starting = False
            self._retention_cancelled_last_run = False
            if self._runtime_retention_done:
                return False
            if self._compaction_hold:
                self._set_retention_held(reason)
                return False
            # A scheduled thread already owns its run id. A direct call must
            # start a new run and clear any event left by the previous pass.
            if not scheduled or not self._retention_run_id:
                self._retention_run_id = f"retention_{uuid.uuid4().hex[:12]}"
            if not scheduled:
                self._retention_cancel.clear()
            self._retention_started_monotonic = started
            self._retention_in_progress = True
            self._last_retention = {
                **self._last_retention,
                "status": "running",
                "phase": "starting",
                "stage": "starting",
                "reason": str(reason or ""),
                "startedAt": started_at,
                "completedAt": "",
                "updatedAt": started_at,
                "durationMs": 0,
                "elapsedMs": 0,
                "deletedRunCount": 0,
                "deletedRowCount": 0,
                "deletedEventCount": 0,
                "deletedSyncRunCount": 0,
                "deletedGenerationCount": 0,
                "processedGenerationCount": 0,
                "totalGenerationCount": 0,
                "totalGenerationRowCount": 0,
                "deletedGenerationRowCount": 0,
                "generationRowCountKnown": False,
                "candidateRunCount": 0,
                "processedRunCount": 0,
                "totalRunCount": 0,
                # The previous pass's nested generation result must not survive
                # into this pass alongside freshly reset outer counters.
                "jobGenerations": {},
                "truncated": False,
                "error": "",
            }

        def report_progress(progress: dict[str, Any]) -> None:
            now = time.monotonic()
            with self._maintenance_lock:
                self._last_retention = {
                    **self._last_retention,
                    **progress,
                    "status": "cancelling" if self._retention_cancel.is_set() else "running",
                    "updatedAt": _now_iso(),
                    "elapsedMs": int(round((now - started) * 1000)),
                }

        try:
            from src.storage.runtime_retention import run_runtime_retention

            status = run_runtime_retention(
                self,
                reason=reason,
                on_progress=report_progress,
                cancel_requested=self._retention_cancel.is_set,
            )
        except RetentionCancelled:
            status = {
                **self._last_retention,
                "status": "cancelled",
                "phase": "cancelled",
                "stage": "cancelled",
                "reason": str(reason or ""),
                "error": "",
            }
            LOGGER.info("SQLite runtime retention cancelled by operator")
        except Exception as exc:
            status = {
                "status": "failed",
                "reason": str(reason or ""),
                "error": str(exc),
                "deletedRunCount": 0,
                "deletedRowCount": 0,
                "processedGenerationCount": 0,
                "totalGenerationCount": 0,
                "totalGenerationRowCount": 0,
                "deletedGenerationRowCount": 0,
                "generationRowCountKnown": False,
            }
            LOGGER.warning("SQLite runtime retention failed: %s", exc)
        elapsed_ms = int(round((time.monotonic() - started) * 1000))
        failed = str(status.get("status") or "") == "failed"
        cancelled = str(status.get("status") or "") == "cancelled"
        if self._retention_cancel.is_set():
            # A request can land after the last batch boundary but before this
            # final status is published. It still wins over a clean finish, and
            # it must also suppress a retry after a failure.
            cancelled = True
            if not failed:
                status = {**status, "status": "cancelled"}
        status = {
            **status,
            "phase": "failed" if failed else "cancelled" if cancelled else "complete",
            "stage": "failed" if failed else "cancelled" if cancelled else "complete",
            "cancelRequested": bool(self._retention_cancel.is_set()),
            "startedAt": started_at,
            "completedAt": _now_iso(),
            "updatedAt": _now_iso(),
            "durationMs": elapsed_ms,
            "elapsedMs": elapsed_ms,
        }
        needs_more = bool(status.get("truncated"))
        with self._maintenance_lock:
            self._last_retention = status
            self._retention_started_monotonic = 0.0
            self._retention_in_progress = False
            self._retention_cancelled_last_run = cancelled
            self._runtime_retention_done = cancelled or (
                str(status.get("status") or "")
                not in {
                    "failed",
                    MAINTENANCE_STATUS_HELD_ACTIVE_TASK,
                }
                and not needs_more
            )
        if (
            not cancelled
            and not needs_more
            and str(status.get("status") or "")
            not in {
                "failed",
                MAINTENANCE_STATUS_HELD_ACTIVE_TASK,
            }
        ):
            # Deleted rows only become free pages; the rewrite is what returns
            # them to the filesystem. Retry on every drained pass, not only on a
            # pass that deleted something: a compaction skipped for space or a
            # busy writer must not wait for the next fetch to become eligible.
            # Wait for the backlog to drain first, because a rebuild that starts
            # while another retention batch is still writing only skips as busy.
            self.schedule_compaction_if_needed(reason=f"{reason}:retention")
        if not cancelled:
            if needs_more:
                self._schedule_retention_followup(DEFAULT_RUNTIME_RETENTION_FOLLOWUP_S)
            elif str(status.get("status") or "") == "failed":
                self._schedule_retention_followup(DEFAULT_RUNTIME_RETENTION_RETRY_S)
            elif str(status.get("status") or "") != MAINTENANCE_STATUS_HELD_ACTIVE_TASK:
                self._schedule_retention_followup(DEFAULT_RUNTIME_RETENTION_INTERVAL_S)
        with self._maintenance_lock:
            if self._retention_thread is threading.current_thread():
                self._retention_thread = None
        return True

    def _schedule_retention_followup(self, delay_s: float) -> None:
        delay = max(0.1, float(delay_s))
        with self._maintenance_lock:
            if self._closed:
                return
            if self._retention_timer is not None:
                return
            timer = threading.Timer(delay, self._retention_timer_fired)
            timer.daemon = True
            timer.name = "baluffo-sqlite-retention-timer"
            self._retention_timer = timer
            self._retention_next_run_monotonic = time.monotonic() + delay
            timer.start()

    def _retention_timer_fired(self) -> None:
        with self._maintenance_lock:
            self._retention_timer = None
            self._retention_next_run_monotonic = 0.0
            self._runtime_retention_done = False
        self.schedule_runtime_retention(reason="retention-timer")

    def schedule_wal_maintenance_if_needed(self, *, reason: str = "") -> bool:
        if self._closed:
            return False
        if self._bulk_delete_mode:
            return False
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
            now = time.monotonic()
            if mode == "PASSIVE":
                cooldown_remaining_ms = self._wal_checkpoint_cooldown_remaining_ms_locked(now)
                if cooldown_remaining_ms > 0:
                    self._last_checkpoint = {
                        **self._last_checkpoint,
                        "status": "cooldown",
                        "reason": str(reason or ""),
                        "walBytes": wal_bytes,
                        "cooldownRemainingMs": cooldown_remaining_ms,
                    }
                    return False
            self._wal_maintenance_cooldown_until_monotonic = now + self.wal_checkpoint_cooldown_s
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
        now = time.monotonic()
        with self._maintenance_lock:
            self._wal_maintenance_cooldown_until_monotonic = now + self.wal_checkpoint_cooldown_s
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
            # Include the checkpoint's own duration in the quiet period. A slow
            # checkpoint must not be followed immediately by another attempt.
            self._wal_maintenance_cooldown_until_monotonic = (
                time.monotonic() + self.wal_checkpoint_cooldown_s
            )
            self._last_checkpoint = status

    @contextmanager
    def _compaction_run(self, reason: str) -> Iterator[None]:
        """Mark a real rebuild as running for health and Admin observers."""
        with self._maintenance_lock:
            self._compaction_in_progress = True
            self._last_vacuum = {
                **self._last_vacuum,
                "status": "running",
                "reason": str(reason or ""),
                "startedAt": _now_iso(),
                "durationMs": 0,
                "reclaimedBytes": 0,
                "error": "",
            }
        try:
            yield
        finally:
            with self._maintenance_lock:
                self._compaction_in_progress = False
                if self._last_vacuum.get("status") == "running":
                    self._last_vacuum = {
                        **self._last_vacuum,
                        "status": "failed",
                        "error": "compaction ended without a terminal status",
                    }

    def vacuum(self, *, reason: str = "") -> dict[str, Any]:
        """Rebuild the database to return free pages to the filesystem.

        An in-place VACUUM holds the write lock for its whole run, which is
        unbounded on a large database, so past `vacuum_large_database_bytes` this
        delegates to the bounded compact-copy rewrite. The rebuild is not
        interruptible: a copy or VACUUM must reach its own terminal state before
        the store can safely continue.
        """
        with self._compaction_run(reason):
            return self._vacuum_locked(reason=reason)

    def _vacuum_locked(self, *, reason: str = "") -> dict[str, Any]:
        """Run the selected rebuild implementation while compaction is marked live."""
        if self._path_size(self.db_path) >= self.vacuum_large_database_bytes:
            return self.compact_database(reason=reason)
        started = time.perf_counter()
        before_bytes = self._path_size(self.db_path)
        last_busy_error: sqlite3.OperationalError | None = None
        for attempt in range(self.busy_retry_attempts):
            try:
                lock_started = time.perf_counter()
                with self._write_lock_guard("vacuum"):
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
                    "method": "vacuum",
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
            "method": "vacuum",
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

    def compact_database(self, *, reason: str = "") -> dict[str, Any]:
        """Shrink the file by rewriting it into a compact copy and swapping it in.

        Post-retention deletes free pages; only a rewrite returns them to the
        filesystem. An in-place VACUUM would hold the store write lock for the
        whole rebuild -- unbounded on a multi-gigabyte database -- so this builds
        the compact copy through `VACUUM INTO` on its own read snapshot while
        writers keep running, then swaps it in under both locks. The run is
        bounded by `compaction_budget_s` and by free disk space, and the original
        file stays in place until the copy validates.
        """
        started = time.monotonic()
        before = self.file_sizes()
        database_bytes = int(before.get("databaseBytes") or 0)
        freelist_bytes = int(before.get("freelistBytes") or 0)
        budget_ms = int(round(self.compaction_budget_s * 1000))
        staging = self.db_path.with_name(
            f"{self.db_path.name}.compact-{os.getpid()}-{uuid.uuid4().hex[:8]}.tmp"
        )
        # The copy needs room for the live pages, not for the current file: a
        # bloated freelist is exactly the case this exists to reclaim.
        live_bytes = max(0, database_bytes - freelist_bytes)
        required_free = live_bytes + DEFAULT_COMPACTION_FREE_SPACE_MARGIN_BYTES
        try:
            free_bytes = int(self._free_disk_bytes())
        except OSError as exc:
            return self._record_rewrite(
                self._rewrite_status(
                    "failed",
                    reason,
                    started,
                    budget_ms,
                    database_bytes,
                    freelist_bytes,
                    error=f"free-space probe failed: {exc}",
                )
            )
        if free_bytes < required_free:
            return self._record_rewrite(
                self._rewrite_status(
                    "skipped-no-space",
                    reason,
                    started,
                    budget_ms,
                    database_bytes,
                    freelist_bytes,
                    freeBytes=free_bytes,
                    requiredBytes=required_free,
                )
            )
        transactions_before = self._write_transactions
        try:
            self._vacuum_into_copy(staging)
            # A copy taken before a committed write would drop that write on
            # swap, so the swap checks the counter again while it holds the lock.
            installed = self._swap_in_compact_copy(
                staging, expected_transactions=transactions_before
            )
        except sqlite3.OperationalError as exc:
            staging.unlink(missing_ok=True)
            interrupted = "interrupt" in str(exc).lower()
            return self._record_rewrite(
                self._rewrite_status(
                    "budget-exceeded" if interrupted else "failed",
                    reason,
                    started,
                    budget_ms,
                    database_bytes,
                    freelist_bytes,
                    error=str(exc),
                )
            )
        except (BaluffoStoreError, OSError, ValueError) as exc:
            staging.unlink(missing_ok=True)
            return self._record_rewrite(
                self._rewrite_status(
                    "failed",
                    reason,
                    started,
                    budget_ms,
                    database_bytes,
                    freelist_bytes,
                    error=str(exc),
                )
            )
        if not installed:
            staging.unlink(missing_ok=True)
            return self._record_rewrite(
                self._rewrite_status(
                    "superseded",
                    reason,
                    started,
                    budget_ms,
                    database_bytes,
                    freelist_bytes,
                    writesDuringCopy=self._write_transactions - transactions_before,
                )
            )
        after = self.file_sizes()
        after_bytes = int(after.get("databaseBytes") or 0)
        return self._record_rewrite(
            self._rewrite_status(
                "ok",
                reason,
                started,
                budget_ms,
                database_bytes,
                freelist_bytes,
                databaseBytes=after_bytes,
                freelistBytes=int(after.get("freelistBytes") or 0),
                afterBytes=after_bytes,
                reclaimedBytes=max(0, database_bytes - after_bytes),
            )
        )

    def _free_disk_bytes(self) -> int:
        return int(shutil.disk_usage(self.data_dir).free)

    def _rewrite_status(
        self,
        status: str,
        reason: str,
        started: float,
        budget_ms: int,
        database_bytes: int,
        freelist_bytes: int,
        **fields: Any,
    ) -> dict[str, Any]:
        return {
            "status": status,
            "method": "vacuum-into",
            "reason": str(reason or ""),
            "durationMs": int(round((time.monotonic() - started) * 1000)),
            "budgetMs": budget_ms,
            "beforeBytes": database_bytes,
            "databaseBytes": database_bytes,
            "freelistBytes": freelist_bytes,
            "reclaimedBytes": 0,
            "error": "",
            **fields,
        }

    def _record_rewrite(self, status: dict[str, Any]) -> dict[str, Any]:
        with self._maintenance_lock:
            self._last_vacuum = status
        return status

    def _vacuum_into_copy(self, staging: Path) -> None:
        """Build a compact copy of the database at ``staging``.

        The copy runs on its own read-only connection, so it takes a read
        snapshot instead of the store write lock and writers keep running. A
        timer interrupts it when the budget expires: an interrupted `VACUUM INTO`
        leaves a partial staging file that the caller removes.
        """
        staging.unlink(missing_ok=True)
        conn = sqlite3.connect(
            f"{self.db_path.as_uri()}?mode=ro",
            uri=True,
            timeout=self.busy_timeout_ms / 1000.0,
        )
        timer: threading.Timer | None = None
        try:
            if self.compaction_budget_s > 0.0:
                timer = threading.Timer(self.compaction_budget_s, conn.interrupt)
                timer.daemon = True
                timer.start()
            conn.execute("VACUUM INTO ?", (str(staging),))
        finally:
            if timer is not None:
                timer.cancel()
            conn.close()

    def _swap_in_compact_copy(self, staging: Path, *, expected_transactions: int) -> bool:
        """Install a validated compact copy, unless a write landed since it began.

        Both locks are held for the swap, so no reader resolves a half-swapped
        path and no writer commits against the file being replaced. A write
        committed after the copy's snapshot is not in the copy, so the swap is
        abandoned instead of dropping it. The stale WAL/SHM files go with the old
        file; the copy already carries the state its snapshot saw.
        """
        with self._write_lock_guard("compact-swap"), self._read_lock:
            if self._write_transactions != expected_transactions:
                return False
            self._validate_database_file(staging)
            if self._reader is not None:
                self._reader.close()
                self._reader = None
            self._writer.close()
            replaced = False
            try:
                os.replace(staging, self.db_path)
                replaced = True
                for suffix in ("-wal", "-shm"):
                    self.db_path.with_name(f"{self.db_path.name}{suffix}").unlink(missing_ok=True)
                self._writer = self._connect()
                self._initialize_writer()
            except BaseException:
                if replaced:
                    self._healthy = False
                    self._last_write_error = "compact copy installed but reopen failed"
                else:
                    self._writer = self._connect()
                raise
            return True

    def write_transaction_count(self) -> int:
        """Committed store write transactions, used to detect a racing write."""
        return int(self._write_transactions)

    def rebuilt_database_status(self) -> dict[str, Any]:
        """Report the retained rollback copy and the last rebuild journal."""
        # Imported here because the leaf imports JobRuntimeStore, which imports
        # this module; a module-level import would close the package cycle.
        from src.storage.current_feed_rebuild import (
            read_rebuild_journal,
            rebuild_backup_path,
            rebuild_journal_path,
        )

        backup = rebuild_backup_path(self.db_path)
        journal = read_rebuild_journal(rebuild_journal_path(self.db_path))
        return {
            "journalPath": str(rebuild_journal_path(self.db_path)),
            "backupPath": str(backup),
            "backupExists": backup.is_file(),
            "backupBytes": self._path_size(backup),
            "journal": journal,
        }

    def install_rebuilt_database(
        self,
        staging: Path | str,
        *,
        expected_transactions: int,
        backup_path: Path | str | None = None,
        journal_path: Path | str | None = None,
        journal_fields: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Swap in a validated current-feed copy and retain the old database.

        The live file is hardlinked to ``backup_path`` before the rename, so the
        rollback copy costs no extra disk. Both locks are held for the swap: no
        reader can resolve a half-swapped path and no writer can commit against
        the file being replaced. A write that landed after the copy's snapshot
        makes the swap ``superseded`` instead of silently dropping it.
        """
        from src.storage.current_feed_rebuild import (
            rebuild_backup_path,
            rebuild_journal_path,
            update_rebuild_journal,
        )

        staging_path = Path(staging)
        target_backup = (
            Path(backup_path) if backup_path is not None else rebuild_backup_path(self.db_path)
        )
        target_journal = (
            Path(journal_path) if journal_path is not None else rebuild_journal_path(self.db_path)
        )
        with self._write_lock_guard("current-feed-rebuild-install"), self._read_lock:
            if self._write_transactions != int(expected_transactions):
                return {
                    "installed": False,
                    "reason": "superseded",
                    "writesDuringCopy": self._write_transactions - int(expected_transactions),
                    "backupPath": str(target_backup),
                    "journalPath": str(target_journal),
                }
            self._validate_database_file(staging_path)
            if target_backup.exists():
                raise BaluffoStoreError(
                    "a rollback copy already exists; delete it or roll back before rebuilding"
                )
            # The rollback copy has no WAL of its own, so every committed frame
            # must be in the main file before the hardlink is created.
            checkpoint = self._writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if int(checkpoint[0] or 0) != 0:
                return {
                    "installed": False,
                    "reason": "checkpoint-busy",
                    "backupPath": str(target_backup),
                    "journalPath": str(target_journal),
                }
            try:
                os.link(self.db_path, target_backup)
            except OSError as exc:
                raise BaluffoStoreError(
                    f"could not create the old-database hardlink: {exc}"
                ) from exc
            before_bytes = self._path_size(self.db_path)
            update_rebuild_journal(
                target_journal,
                status="backup-created",
                phase="backup-created",
                backupPath=str(target_backup),
                backupBytes=before_bytes,
                **(journal_fields or {}),
            )
            if self._reader is not None:
                self._reader.close()
                self._reader = None
            self._writer.close()
            replaced = False
            try:
                os.replace(staging_path, self.db_path)
                replaced = True
                for suffix in ("-wal", "-shm"):
                    self.db_path.with_name(f"{self.db_path.name}{suffix}").unlink(missing_ok=True)
                self._writer = self._connect()
                self._initialize_writer()
            except BaseException:
                if replaced:
                    self._healthy = False
                    self._last_write_error = (
                        "current-feed rebuild installed but the store could not reopen"
                    )
                else:
                    self._writer = self._connect()
                raise
            after_bytes = self._path_size(self.db_path)
            result = {
                "installed": True,
                "reason": "",
                "backupPath": str(target_backup),
                "backupBytes": before_bytes,
                "beforeBytes": before_bytes,
                "databaseBytes": after_bytes,
                "reclaimedBytes": max(0, before_bytes - after_bytes),
                "journalPath": str(target_journal),
            }
            update_rebuild_journal(
                target_journal,
                status="complete",
                phase="complete",
                backupPath=str(target_backup),
                backupBytes=before_bytes,
                databaseBytes=after_bytes,
                reclaimedBytes=result["reclaimedBytes"],
                **(journal_fields or {}),
            )
            return result

    def restore_rebuilt_database(
        self,
        backup_path: Path | str | None = None,
        *,
        journal_path: Path | str | None = None,
    ) -> dict[str, Any]:
        """Reinstate the retained pre-rebuild database.

        A rollback consumes the hardlink: the old database becomes live again and
        the backup path disappears. ``beforeBytes`` is the size of the rebuilt
        database that is being replaced.
        """
        from src.storage.current_feed_rebuild import (
            rebuild_backup_path,
            rebuild_journal_path,
            update_rebuild_journal,
        )

        target_backup = (
            Path(backup_path) if backup_path is not None else rebuild_backup_path(self.db_path)
        )
        target_journal = (
            Path(journal_path) if journal_path is not None else rebuild_journal_path(self.db_path)
        )
        with self._write_lock_guard("current-feed-rebuild-rollback"), self._read_lock:
            if not target_backup.is_file():
                raise BaluffoStoreError("no retained pre-rebuild database to restore")
            self._validate_database_file(target_backup)
            before_bytes = self._path_size(self.db_path)
            if self._reader is not None:
                self._reader.close()
                self._reader = None
            self._writer.close()
            replaced = False
            try:
                os.replace(target_backup, self.db_path)
                replaced = True
                for suffix in ("-wal", "-shm"):
                    self.db_path.with_name(f"{self.db_path.name}{suffix}").unlink(missing_ok=True)
                self._writer = self._connect()
                self._initialize_writer()
            except BaseException:
                if replaced:
                    self._healthy = False
                    self._last_write_error = (
                        "pre-rebuild database restored but the store could not reopen"
                    )
                else:
                    self._writer = self._connect()
                raise
            after_bytes = self._path_size(self.db_path)
            result = {
                "restored": True,
                "backupPath": str(target_backup),
                "beforeBytes": before_bytes,
                "databaseBytes": after_bytes,
                "journalPath": str(target_journal),
            }
            update_rebuild_journal(
                target_journal,
                status="rolled-back",
                phase="rolled-back",
                restoredAt=_now_iso(),
                databaseBytes=after_bytes,
            )
            return result

    def compaction_status(self, *, sizes: dict[str, int] | None = None) -> dict[str, Any]:
        """Report the configured gate and the observed reason it is not due."""
        current_sizes = dict(sizes) if sizes is not None else self.file_sizes()
        database_bytes = max(0, int(current_sizes.get("databaseBytes") or 0))
        freelist_bytes = max(0, int(current_sizes.get("freelistBytes") or 0))
        allocated_bytes = max(0, database_bytes - freelist_bytes)
        observed_ratio = freelist_bytes / database_bytes if database_bytes > 0 else 0.0
        effective_ratio = self.effective_vacuum_freelist_ratio(database_bytes)
        vacuum_due_by_size = self._compaction_needed(current_sizes)
        vacuum_blocked_by = ""
        if not vacuum_due_by_size:
            if freelist_bytes < self.vacuum_freelist_threshold_bytes:
                vacuum_blocked_by = "freelist-bytes"
            elif observed_ratio < effective_ratio:
                vacuum_blocked_by = "freelist-ratio"
        space = {
            "databaseBytes": database_bytes,
            "allocatedBytes": allocated_bytes,
            "freelistBytes": freelist_bytes,
            "observedFreelistRatio": round(observed_ratio, 4),
            "effectiveFreelistRatio": effective_ratio,
            "vacuumDueBySize": vacuum_due_by_size,
            "vacuumBlockedBy": vacuum_blocked_by,
            "largeAllocatedLowFreelist": bool(
                allocated_bytes >= self.vacuum_large_database_bytes
                and observed_ratio < effective_ratio
            ),
        }
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
                "compactionBudgetMs": int(round(self.compaction_budget_s * 1000)),
                "running": bool(self._compaction_in_progress),
                "interruptible": False,
                "heldByActiveTask": self._compaction_hold,
                "lastWriteAgoS": round(max(0.0, time.monotonic() - self._last_write_monotonic), 3),
                "space": space,
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
        if self._closed:
            return False
        if self._bulk_delete_mode:
            return False
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
            with self._write_lock_guard("backup"):
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

    def table_sizes(self) -> dict[str, Any]:
        """Bounded per-table space breakdown, cached between health reads.

        The measurement samples rowids rather than traversing the database, so a
        multi-hundred-gigabyte runtime store answers in bounded time. The payload
        states how it was obtained (`method`, `estimated`, `complete`, `budgetMs`),
        so an estimate is never presented as an exact measurement.
        """
        now = time.monotonic()
        with self._table_sizes_lock:
            snapshot = self._table_sizes_snapshot
            if snapshot is not None and now < self._table_sizes_cache_until_monotonic:
                return snapshot
            # Imported here because the storage package re-exports BaluffoStore:
            # a module-level import would close the package cycle.
            from src.storage.db_table_sizes import measure_table_sizes

            snapshot = measure_table_sizes(self.db_path)
            self._table_sizes_snapshot = snapshot
            cache_s = (
                DEFAULT_TABLE_SIZE_CACHE_S
                if snapshot.get("complete") is True
                else DEFAULT_TABLE_SIZE_PARTIAL_CACHE_S
            )
            self._table_sizes_cache_until_monotonic = time.monotonic() + cache_s
            return snapshot

    def health(self) -> dict[str, Any]:
        health_started = time.perf_counter()
        quick_check, quick_check_deferred = self.quick_check_for_health()
        migration_version = self.migration_version()
        busy_rate = self._busy_count / max(1, self._write_attempts)
        sizes = self.file_sizes()
        quick_check_ok = quick_check == "ok" or quick_check_deferred
        lock_started = time.perf_counter()
        wal_mode, wal_mode_source = self._pragma_with_source(
            "PRAGMA journal_mode", self._last_known_wal_mode
        )
        foreign_keys, foreign_keys_source = self._pragma_with_source(
            "PRAGMA foreign_keys", self._last_known_foreign_keys
        )
        lock_wait_ms = int(round((time.perf_counter() - lock_started) * 1000))
        return {
            "schemaVersion": STORAGE_SCHEMA_VERSION,
            "databasePath": str(self.db_path),
            **sizes,
            "migrationVersion": migration_version,
            "walMode": str(wal_mode or "").lower(),
            "walModeSource": wal_mode_source,
            "foreignKeys": int(foreign_keys or 0),
            "foreignKeysSource": foreign_keys_source,
            "healthLockBusy": "cached-busy" in {wal_mode_source, foreign_keys_source},
            "healthLockTimeoutMs": self.health_lock_timeout_ms,
            "healthTiming": {
                "lockWaitMs": lock_wait_ms,
                "healthMs": int(round((time.perf_counter() - health_started) * 1000)),
            },
            "quickCheck": quick_check,
            "quickCheckScope": "deferred" if quick_check_deferred else "limited",
            "quickCheckDeferred": quick_check_deferred,
            "quickCheckMaxDatabaseBytes": self.quick_check_max_database_bytes,
            "healthy": bool(self._healthy and quick_check_ok),
            "lastWriteError": self._last_write_error,
            "writeLock": self.write_lock_status(),
            "busyCount": self._busy_count,
            "busyRate": busy_rate,
            "writeAttempts": self._write_attempts,
            "writeTransactions": self._write_transactions,
            "walMaintenance": self.wal_maintenance_status(),
            "compaction": self.compaction_status(sizes=sizes),
            "runtimeRetention": self.runtime_retention_status(),
            "currentFeedRebuild": self.rebuilt_database_status(),
            "tableSizes": self.table_sizes(),
            "authorityModes": self.get_authority_modes(),
        }
