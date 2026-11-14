#!/usr/bin/env python3
"""Profile jobs-generation cleanup on a fixed, disposable SQLite fixture.

This is the tracked version of the ad hoc 2026-11-13 cleanup harness. It builds
fixtures from the shipped migrations and seed path, runs the real
``JobRuntimeStore.cleanup_old_generations`` path on copies, and records each
delete transaction's DELETE, commit, WAL growth, and checkpoint cost.

The fixture is generated in code, not read from a live feed or an ignored
``_out`` artifact: the row template, counts, timestamps and generations are
``FIXTURE_VERSION``-pinned. The default fixture is one old generation of
39,518 jobs with four source rows per job plus a one-row current generation.

Usage::

    python tools/measurements/pipeline/generation_cleanup_profile.py
    python tools/measurements/pipeline/generation_cleanup_profile.py \\
        --job-count 5000 --sources-per-job 4 --arms baseline,no-auto-checkpoint

Each invocation writes ``<output-root>/<UTC-stamp>/`` with ``results.json``,
``SUMMARY.md`` and (unless ``--keep-fixtures``) no databases. The default
``baseline`` arm uses the shipped 80 MiB bulk-delete auto-checkpoint,
``no-auto-checkpoint`` disables it, and ``large-payload`` repeats ``baseline``
with a 16 KiB job description. Read ``maxDeleteMs`` against ``maxCommitMs``:
SQLite's inline auto-checkpoint is inside the commit and cannot be timed
separately.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import sqlite3
import statistics
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psutil

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import src.storage.baluffo_store as baluffo_store_module  # noqa: E402
from src.storage import BaluffoStore, JobRuntimeStore  # noqa: E402

FIXTURE_VERSION = 1
FIXTURE_OLD_GENERATION = "benchmark_old"
FIXTURE_CURRENT_GENERATION = "benchmark_current"
FIXTURE_TIMESTAMP = "2026-01-01T00:00:00Z"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / ".tmp" / "generation-cleanup-profile"
DEFAULT_JOB_COUNT = 39_518
DEFAULT_SOURCES_PER_JOB = 4
DEFAULT_LARGE_PAYLOAD_BYTES = 16 * 1024
DEFAULT_DELETE_BATCH_SIZE = 1_000
ARM_CHOICES = ("baseline", "no-auto-checkpoint", "large-payload")
SLOW_BATCH_SECONDS = 0.25

FixtureLogger = Callable[[str], None]


def _timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _printer(message: str) -> None:
    print(f"[{_timestamp()}] {message}", flush=True)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _fixture_description(extra_payload_bytes: int) -> str:
    if extra_payload_bytes:
        return "x" * extra_payload_bytes
    return "Deterministic generation-cleanup fixture; no live data. " * 12


def build_fixture_rows(
    *,
    job_count: int,
    sources_per_job: int,
    extra_payload_bytes: int = 0,
    index_offset: int = 0,
) -> list[dict[str, Any]]:
    """Build deterministic job rows for one feed generation.

    The shape deliberately includes the fields the runtime feed stores and a
    fixed description, so the fixture can be compared across runs by
    ``fixture_rows_sha256``. No row is copied from live data.
    """
    if job_count < 1:
        raise ValueError("job_count must be positive")
    if sources_per_job < 0:
        raise ValueError("sources_per_job must be non-negative")
    if extra_payload_bytes < 0:
        raise ValueError("extra_payload_bytes must be non-negative")

    description = _fixture_description(extra_payload_bytes)
    rows: list[dict[str, Any]] = []
    for offset in range(job_count):
        index = index_offset + offset
        source_bundle = [
            {
                "sourceId": f"benchmark_source_{source_index}",
                "source": f"benchmark_source_{source_index}",
                "sourceJobId": f"benchmark_source_{source_index}:{index}",
                "sourceName": f"Benchmark Source {source_index}",
                "jobLink": f"https://bench.invalid/sources/{source_index}/{index}",
            }
            for source_index in range(sources_per_job)
        ]
        rows.append(
            {
                "id": index + 1,
                "title": f"Benchmark Game Artist {index}",
                "company": "Benchmark Studio",
                "city": "Amsterdam",
                "country": "NL",
                "workType": "Hybrid",
                "contractType": "Full-time",
                "jobLink": f"https://bench.invalid/jobs/{index}",
                "sector": "Game",
                "profession": "game-development",
                "locations": [{"city": "Amsterdam", "country": "NL"}],
                "locationSummary": "Amsterdam, NL",
                "description": description,
                "source": "benchmark_fixture",
                "postedAt": FIXTURE_TIMESTAMP,
                "status": "active",
                "firstSeenAt": FIXTURE_TIMESTAMP,
                "lastSeenAt": FIXTURE_TIMESTAMP,
                "removedAt": "",
                "lifecycleEvent": "",
                "lifecycleReason": "",
                "availabilityId": f"availability_benchmark_{index}",
                "availabilityStatus": "available",
                "availabilityCheckedAt": FIXTURE_TIMESTAMP,
                "availabilityVerifiedAt": FIXTURE_TIMESTAMP,
                "availabilityUnavailableAt": "",
                "availabilityEvidence": {
                    "kind": "source_present",
                    "confidence": "definitive",
                    "checkedAt": FIXTURE_TIMESTAMP,
                    "source": "benchmark_fixture",
                },
                "qualityScore": 100,
                "focusScore": 100,
                "sourceBundleCount": len(source_bundle),
                "sourceBundle": source_bundle,
            }
        )
    return rows


def fixture_rows_sha256(rows: Sequence[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(_json_dumps(row).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _sqlite_metadata(db_path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{db_path.resolve().as_posix()}?mode=ro", uri=True)
    wal_path = db_path.with_name(f"{db_path.name}-wal")
    try:
        conn.execute("PRAGMA query_only=ON")
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0] or 0)
        page_count = int(conn.execute("PRAGMA page_count").fetchone()[0] or 0)
        freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0] or 0)
        table_counts: dict[str, int] = {}
        for table in ("jobs", "job_sources", "job_feed_generation_stats"):
            table_counts[table] = int(
                conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] or 0
            )
        payload_lengths: dict[str, dict[str, int]] = {}
        for table in ("jobs", "job_sources"):
            row = conn.execute(
                f"""
                SELECT COUNT(*), COALESCE(SUM(length(payload_json)), 0),
                       COALESCE(MIN(length(payload_json)), 0),
                       COALESCE(MAX(length(payload_json)), 0)
                FROM "{table}"
                """
            ).fetchone()
            payload_lengths[table] = {
                "count": int(row[0] or 0),
                "payloadBytes": int(row[1] or 0),
                "minPayloadBytes": int(row[2] or 0),
                "maxPayloadBytes": int(row[3] or 0),
            }
        allocations: dict[str, int] = {}
        try:
            for name, size in conn.execute(
                "SELECT name, SUM(pgsize) FROM dbstat GROUP BY name ORDER BY name"
            ):
                allocations[str(name)] = int(size or 0)
        except sqlite3.OperationalError:
            allocations = {}
        state = conn.execute(
            "SELECT current_generation, row_count, source_count FROM job_feed_state WHERE id=1"
        ).fetchone()
        generation_stats = [
            {
                "generation": str(row[0]),
                "rowCount": int(row[1] or 0),
                "sourceCount": int(row[2] or 0),
            }
            for row in conn.execute(
                "SELECT feed_generation, row_count, source_count "
                "FROM job_feed_generation_stats ORDER BY updated_at"
            )
        ]
        return {
            "databaseBytes": db_path.stat().st_size,
            "walBytes": wal_path.stat().st_size if wal_path.exists() else 0,
            "pageSize": page_size,
            "pageCount": page_count,
            "freelistCount": freelist_count,
            "freelistBytes": freelist_count * page_size,
            "tableCounts": table_counts,
            "payloadLengths": payload_lengths,
            "allocations": allocations,
            "currentGeneration": {
                "generation": str(state[0] or "") if state else "",
                "rowCount": int(state[1] or 0) if state else 0,
                "sourceCount": int(state[2] or 0) if state else 0,
            },
            "generationStats": generation_stats,
        }
    finally:
        conn.close()


def _checkpoint_to_zero(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()


def build_database(
    destination: Path,
    *,
    job_count: int,
    sources_per_job: int,
    extra_payload_bytes: int,
    log: FixtureLogger = _printer,
) -> dict[str, Any]:
    """Build one fixed old-generation + current-generation database."""
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)
    started = time.perf_counter()
    log(f"building {destination.name} extra_payload_bytes={extra_payload_bytes}")
    rows = build_fixture_rows(
        job_count=job_count,
        sources_per_job=sources_per_job,
        extra_payload_bytes=extra_payload_bytes,
    )
    current_rows = build_fixture_rows(
        job_count=1,
        sources_per_job=1,
        extra_payload_bytes=extra_payload_bytes,
        index_offset=job_count + 10,
    )
    with BaluffoStore(
        destination,
        defer_maintenance=True,
        slow_write_warn_ms=0,
    ) as store:
        runtime = JobRuntimeStore(store, delete_batch_size=DEFAULT_DELETE_BATCH_SIZE)
        runtime.replace_feed(
            run_id="benchmark_old",
            rows=rows,
            generation=FIXTURE_OLD_GENERATION,
        )
        runtime.replace_feed(
            run_id="benchmark_current",
            rows=current_rows,
            generation=FIXTURE_CURRENT_GENERATION,
        )
        store.checkpoint_required(mode="TRUNCATE")
    db_path = destination / "baluffo-runtime.db"
    _checkpoint_to_zero(db_path)
    metadata = _sqlite_metadata(db_path)
    expected_old_rows = job_count + job_count * sources_per_job
    expected_current_rows = 2
    metadata.update(
        {
            "databasePath": str(db_path),
            "fixtureVersion": FIXTURE_VERSION,
            "oldGeneration": FIXTURE_OLD_GENERATION,
            "currentGeneration": FIXTURE_CURRENT_GENERATION,
            "jobCount": job_count,
            "sourcesPerJob": sources_per_job,
            "extraPayloadBytes": extra_payload_bytes,
            "expectedOldRows": expected_old_rows,
            "expectedCurrentRows": expected_current_rows,
            "fixtureRowsSha256": fixture_rows_sha256([*rows, *current_rows]),
            "buildSeconds": round(time.perf_counter() - started, 3),
        }
    )
    log(
        f"built {destination.name}: {metadata['databaseBytes'] / (1024 * 1024):.1f} MiB "
        f"in {metadata['buildSeconds']}s"
    )
    return metadata


def _snapshot(db_path: Path, process: psutil.Process) -> dict[str, int | float]:
    wal_path = db_path.with_name(f"{db_path.name}-wal")
    shm_path = db_path.with_name(f"{db_path.name}-shm")
    process_io = process.io_counters()
    disk_io = psutil.disk_io_counters()
    cpu = process.cpu_times()
    return {
        "dbBytes": db_path.stat().st_size if db_path.exists() else 0,
        "walBytes": wal_path.stat().st_size if wal_path.exists() else 0,
        "shmBytes": shm_path.stat().st_size if shm_path.exists() else 0,
        "processReadBytes": int(process_io.read_bytes),
        "processWriteBytes": int(process_io.write_bytes),
        "processReadCount": int(process_io.read_count),
        "processWriteCount": int(process_io.write_count),
        "diskReadBytes": int(disk_io.read_bytes) if disk_io else 0,
        "diskWriteBytes": int(disk_io.write_bytes) if disk_io else 0,
        "diskReadTimeMs": int(disk_io.read_time) if disk_io else 0,
        "diskWriteTimeMs": int(disk_io.write_time) if disk_io else 0,
        "processCpuUserSeconds": float(cpu.user),
        "processCpuSystemSeconds": float(cpu.system),
    }


def _delta(
    before: Mapping[str, int | float], after: Mapping[str, int | float]
) -> dict[str, int | float]:
    keys = (
        "dbBytes",
        "walBytes",
        "shmBytes",
        "processReadBytes",
        "processWriteBytes",
        "processReadCount",
        "processWriteCount",
        "diskReadBytes",
        "diskWriteBytes",
        "diskReadTimeMs",
        "diskWriteTimeMs",
        "processCpuUserSeconds",
        "processCpuSystemSeconds",
    )
    return {key: after[key] - before[key] for key in keys}


def _trace_callback_factory(state: dict[str, Any]) -> Callable[[str], None]:
    def trace_callback(sql: str) -> None:
        normalized = sql.lstrip().upper()
        now = time.perf_counter()
        if normalized.startswith("DELETE"):
            state["delete"] = now
            state["sql"] = sql[:300]
        elif normalized.startswith("COMMIT"):
            state["commit"] = now

    return trace_callback


def run_arm(
    *,
    arm_name: str,
    base_db: Path,
    work_dir: Path,
    disable_auto_checkpoint: bool,
    delete_batch_size: int,
    expected_deleted_rows: int,
    log: FixtureLogger = _printer,
) -> dict[str, Any]:
    """Run one cleanup arm on a fresh copy of ``base_db``."""
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True)
    db_path = work_dir / "baluffo-runtime.db"
    shutil.copy2(base_db, db_path)
    _checkpoint_to_zero(db_path)
    before_metadata = _sqlite_metadata(db_path)
    process = psutil.Process(os.getpid())
    batches: list[dict[str, Any]] = []
    checkpoints: list[dict[str, Any]] = []
    progress: list[dict[str, Any]] = []
    trace_state: dict[str, Any] = {"delete": None, "commit": None, "sql": ""}
    previous_auto = baluffo_store_module.DEFAULT_BULK_DELETE_WAL_AUTOCHECKPOINT_PAGES
    auto_checkpoint_pages = 0 if disable_auto_checkpoint else previous_auto
    if disable_auto_checkpoint:
        baluffo_store_module.DEFAULT_BULK_DELETE_WAL_AUTOCHECKPOINT_PAGES = 0
    store: BaluffoStore | None = None
    original_checkpoint: Callable[..., dict[str, int]] | None = None
    started = time.perf_counter()
    try:
        store = BaluffoStore(
            work_dir,
            defer_maintenance=True,
            slow_write_warn_ms=0,
        )
        runtime = JobRuntimeStore(store, delete_batch_size=delete_batch_size)
        original_batch = runtime._delete_generation_batch
        original_checkpoint = store.checkpoint_required
        store._writer.set_trace_callback(_trace_callback_factory(trace_state))

        def measured_batch(table: str, sql: str, parameters: tuple[Any, ...]) -> int:
            trace_state.update({"delete": None, "commit": None, "sql": sql[:300]})
            before = _snapshot(db_path, process)
            started_batch = time.perf_counter()
            deleted = int(original_batch(table, sql, parameters))
            finished_batch = time.perf_counter()
            after = _snapshot(db_path, process)
            delete_started = trace_state.get("delete")
            commit_started = trace_state.get("commit")
            delete_seconds = (
                max(0.0, float(commit_started) - float(delete_started))
                if delete_started is not None and commit_started is not None
                else None
            )
            commit_seconds = (
                max(0.0, float(finished_batch) - float(commit_started))
                if commit_started is not None
                else None
            )
            deltas = _delta(before, after)
            batches.append(
                {
                    "batch": len(batches) + 1,
                    "table": table,
                    "deletedRows": deleted,
                    "totalSeconds": round(finished_batch - started_batch, 6),
                    "preDeleteSeconds": (
                        round(max(0.0, float(delete_started) - started_batch), 6)
                        if delete_started is not None
                        else None
                    ),
                    "deleteSeconds": (
                        round(delete_seconds, 6) if delete_seconds is not None else None
                    ),
                    "commitSeconds": (
                        round(commit_seconds, 6) if commit_seconds is not None else None
                    ),
                    "walBytesBefore": before["walBytes"],
                    "walBytesAfter": after["walBytes"],
                    "walGrowthBytes": after["walBytes"] - before["walBytes"],
                    **{f"{key}Before": value for key, value in before.items()},
                    **{f"{key}After": value for key, value in after.items()},
                    **{f"delta{key[0].upper()}{key[1:]}": value for key, value in deltas.items()},
                }
            )
            return deleted

        def measured_checkpoint(*, mode: str = "TRUNCATE") -> dict[str, int]:
            if original_checkpoint is None:
                raise RuntimeError("checkpoint callback was not captured")
            before = _snapshot(db_path, process)
            started_checkpoint = time.perf_counter()
            result = original_checkpoint(mode=mode)
            finished_checkpoint = time.perf_counter()
            after = _snapshot(db_path, process)
            deltas = _delta(before, after)
            checkpoints.append(
                {
                    "mode": mode,
                    "checkpointSeconds": round(finished_checkpoint - started_checkpoint, 6),
                    "result": dict(result),
                    "walBytesBefore": before["walBytes"],
                    "walBytesAfter": after["walBytes"],
                    "walGrowthBytes": after["walBytes"] - before["walBytes"],
                    "processWriteBytes": deltas["processWriteBytes"],
                    "diskWriteTimeMs": deltas["diskWriteTimeMs"],
                }
            )
            return result

        runtime._delete_generation_batch = measured_batch  # type: ignore[method-assign]
        store.checkpoint_required = measured_checkpoint  # type: ignore[method-assign]
        log(f"running {arm_name}: auto_checkpoint_pages={auto_checkpoint_pages}")
        cleanup_started = time.perf_counter()
        deleted_generations = runtime.cleanup_old_generations(
            delete_cap=1,
            on_progress=progress.append,
        )
        cleanup_seconds = time.perf_counter() - cleanup_started
        store._writer.set_trace_callback(None)
        if original_checkpoint is not None:
            store.checkpoint_required = original_checkpoint  # type: ignore[method-assign]
        after_metadata = _sqlite_metadata(db_path)
        deleted_rows = sum(int(row["deletedRows"]) for row in batches)
        result = {
            "arm": arm_name,
            "config": {
                "autoCheckpointPages": auto_checkpoint_pages,
                "deleteBatchSize": delete_batch_size,
                "deleteCap": 1,
                "bulkDeleteMode": True,
                "secureDelete": baluffo_store_module.BULK_DELETE_SECURE_DELETE,
            },
            "deletedGenerations": deleted_generations,
            "deletedRows": deleted_rows,
            "expectedDeletedRows": expected_deleted_rows,
            "deletedRowsMatch": deleted_rows == expected_deleted_rows,
            "cleanupSeconds": round(cleanup_seconds, 3),
            "totalSeconds": round(time.perf_counter() - started, 3),
            "before": before_metadata,
            "after": after_metadata,
            "remainingCounts": {
                "jobs": after_metadata["tableCounts"]["jobs"],
                "job_sources": after_metadata["tableCounts"]["job_sources"],
            },
            "batchCount": len(batches),
            "batches": batches,
            "checkpoints": checkpoints,
            "progress": progress,
        }
        log(
            f"finished {arm_name}: {len(batches)} batches, {deleted_rows} rows, "
            f"{cleanup_seconds:.2f}s cleanup"
        )
        return result
    finally:
        if store is not None:
            try:
                store._writer.set_trace_callback(None)
            except (sqlite3.Error, OSError):
                pass
            if original_checkpoint is not None:
                store.checkpoint_required = original_checkpoint  # type: ignore[method-assign]
            try:
                store.close()
            except (OSError, RuntimeError, sqlite3.Error) as exc:  # pragma: no cover
                log(f"close warning for {arm_name}: {exc}")
        baluffo_store_module.DEFAULT_BULK_DELETE_WAL_AUTOCHECKPOINT_PAGES = previous_auto


def _percentile(values: Sequence[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
    return ordered[index]


def _pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    if len(xs) < 2 or len(xs) != len(ys):
        return None
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=True))
    denom_x = sum((x - mean_x) ** 2 for x in xs) ** 0.5
    denom_y = sum((y - mean_y) ** 2 for y in ys) ** 0.5
    if denom_x == 0 or denom_y == 0:
        return None
    return numerator / (denom_x * denom_y)


def _seconds_stats(values: Sequence[float]) -> dict[str, float]:
    if not values:
        return {"min": 0.0, "median": 0.0, "max": 0.0}
    return {
        "min": round(min(values), 6),
        "median": round(statistics.median(values), 6),
        "max": round(max(values), 6),
    }


def summarize_arm(arm: Mapping[str, Any]) -> dict[str, Any]:
    batches = [row for row in arm["batches"] if isinstance(row, Mapping)]
    total_values = [float(row["totalSeconds"]) for row in batches]
    delete_values = [
        float(row["deleteSeconds"]) for row in batches if row["deleteSeconds"] is not None
    ]
    commit_values = [
        float(row["commitSeconds"]) for row in batches if row["commitSeconds"] is not None
    ]
    wal_values = [float(row["walGrowthBytes"]) for row in batches]
    by_table: dict[str, Any] = {}
    for table in ("job_sources", "jobs"):
        rows = [row for row in batches if row["table"] == table]
        by_table[table] = {
            "batches": len(rows),
            "deletedRows": sum(int(row["deletedRows"]) for row in rows),
            "totalSeconds": _seconds_stats([float(row["totalSeconds"]) for row in rows]),
            "deleteSeconds": _seconds_stats(
                [float(row["deleteSeconds"]) for row in rows if row["deleteSeconds"] is not None]
            ),
            "commitSeconds": _seconds_stats(
                [float(row["commitSeconds"]) for row in rows if row["commitSeconds"] is not None]
            ),
            "writesMiB": round(
                sum(int(row["deltaProcessWriteBytes"]) for row in rows) / (1024 * 1024), 2
            ),
        }
    slowest = [
        {
            "batch": row["batch"],
            "table": row["table"],
            "deletedRows": row["deletedRows"],
            "totalSeconds": row["totalSeconds"],
            "deleteSeconds": row["deleteSeconds"],
            "commitSeconds": row["commitSeconds"],
            "walMiBBefore": round(float(row["walBytesBefore"]) / (1024 * 1024), 2),
            "walMiBAfter": round(float(row["walBytesAfter"]) / (1024 * 1024), 2),
            "walGrowthBytes": row["walGrowthBytes"],
            "processWriteMiB": round(float(row["deltaProcessWriteBytes"]) / (1024 * 1024), 2),
            "diskWriteTimeMs": row["deltaDiskWriteTimeMs"],
        }
        for row in sorted(batches, key=lambda item: float(item["totalSeconds"]), reverse=True)[:10]
    ]
    checkpoints = [
        {
            "mode": row["mode"],
            "checkpointSeconds": row["checkpointSeconds"],
            "walBytesBefore": row["walBytesBefore"],
            "walBytesAfter": row["walBytesAfter"],
            "walGrowthBytes": row["walGrowthBytes"],
        }
        for row in arm["checkpoints"]
    ]
    return {
        "arm": arm["arm"],
        "cleanupSeconds": arm["cleanupSeconds"],
        "batchCount": arm["batchCount"],
        "deletedRows": arm["deletedRows"],
        "expectedDeletedRows": arm["expectedDeletedRows"],
        "deletedRowsMatch": arm["deletedRowsMatch"],
        "batchSeconds": _seconds_stats(total_values),
        "batchSecondsMax": round(max(total_values, default=0.0), 3),
        "batchSecondsP95": round(_percentile(total_values, 0.95), 3),
        "deleteSeconds": _seconds_stats(delete_values),
        "commitSeconds": _seconds_stats(commit_values),
        "walGrowthBytes": {
            "min": int(min(wal_values)) if wal_values else 0,
            "max": int(max(wal_values)) if wal_values else 0,
        },
        "slowBatchCount025s": sum(
            1 for row in batches if float(row["totalSeconds"]) >= SLOW_BATCH_SECONDS
        ),
        "processWriteMiB": round(
            sum(float(row["deltaProcessWriteBytes"]) for row in batches) / (1024 * 1024),
            2,
        ),
        "diskWriteTimeMs": sum(int(row["deltaDiskWriteTimeMs"]) for row in batches),
        "commitVsProcessWriteCorrelation": (
            round(value, 3)
            if (
                value := _pearson(
                    commit_values,
                    [float(row["deltaProcessWriteBytes"]) for row in batches],
                )
            )
            is not None
            else None
        ),
        "totalVsDeleteCorrelation": (
            round(value, 3)
            if (
                value := _pearson(
                    total_values,
                    delete_values,
                )
            )
            is not None
            else None
        ),
        "checkpoints": checkpoints,
        "byTable": by_table,
        "slowest": slowest,
    }


def _summary_markdown(results: Mapping[str, Any]) -> str:
    lines = [
        "# Generation cleanup profile",
        "",
        f"- Fixture version: `{results['fixtureVersion']}`",
        f"- Generated: `{results['finishedAt']}`",
        f"- Host: `{results['environment']['platform']}`, Python `{results['environment']['python']}`, SQLite `{results['environment']['sqlite']}`",
        f"- Fixture: **{results['config']['jobCount']:,} jobs** x "
        f"**{results['config']['sourcesPerJob']} sources**, "
        f"{results['config']['deleteBatchSize']:,}-row delete batches",
        "",
        "| Arm | Deleted rows | Batches | Cleanup s | Max batch ms | p95 ms | Max DELETE ms | Max commit ms | Max WAL growth MiB | Checkpoint max ms |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, summary in results["summaries"].items():
        checkpoint_max = max(
            (float(row["checkpointSeconds"]) for row in summary["checkpoints"]),
            default=0.0,
        )
        wal_max = int(summary["walGrowthBytes"]["max"]) / (1024 * 1024)
        lines.append(
            f"| `{name}` | {summary['deletedRows']:,} | {summary['batchCount']} | "
            f"{summary['cleanupSeconds']:.2f} | {summary['batchSeconds']['max'] * 1000:.1f} | "
            f"{summary['batchSecondsP95'] * 1000:.1f} | "
            f"{summary['deleteSeconds']['max'] * 1000:.1f} | "
            f"{summary['commitSeconds']['max'] * 1000:.1f} | {wal_max:.2f} | "
            f"{checkpoint_max * 1000:.1f} |"
        )
    lines.extend(
        [
            "",
            "`commitMs` includes SQLite's inline auto-checkpoint. Explicit",
            "`checkpoint:*` rows carry their own checkpoint time; SQLite does not",
            "expose the inline portion separately.",
            "",
            "Per-batch records, generation metadata and fixture digests are in",
            "`results.json`. Fixture databases are removed unless `--keep-fixtures`",
            "was passed.",
            "",
        ]
    )
    return "\n".join(lines)


def _parse_arms(raw: str) -> tuple[str, ...]:
    names = tuple(dict.fromkeys(part.strip() for part in raw.split(",") if part.strip()))
    invalid = [name for name in names if name not in ARM_CHOICES]
    if not names or invalid:
        raise ValueError(
            f"arms must be a comma-separated subset of {', '.join(ARM_CHOICES)}; got {raw!r}"
        )
    return names


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="run root; each invocation writes a UTC-stamped directory underneath it",
    )
    parser.add_argument(
        "--arms",
        default=",".join(ARM_CHOICES),
        help="comma-separated arms: baseline,no-auto-checkpoint,large-payload",
    )
    parser.add_argument("--job-count", type=int, default=DEFAULT_JOB_COUNT)
    parser.add_argument("--sources-per-job", type=int, default=DEFAULT_SOURCES_PER_JOB)
    parser.add_argument("--delete-batch-size", type=int, default=DEFAULT_DELETE_BATCH_SIZE)
    parser.add_argument(
        "--large-payload-bytes",
        type=int,
        default=DEFAULT_LARGE_PAYLOAD_BYTES,
        help="description size added by the large-payload arm",
    )
    parser.add_argument(
        "--keep-fixtures",
        action="store_true",
        help="keep the built databases and arm copies (about 2.3 GB at default size)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.job_count < 1:
        raise SystemExit("--job-count must be positive")
    if args.sources_per_job < 0:
        raise SystemExit("--sources-per-job must be non-negative")
    if args.delete_batch_size < 1:
        raise SystemExit("--delete-batch-size must be positive")
    if args.large_payload_bytes < 0:
        raise SystemExit("--large-payload-bytes must be non-negative")
    try:
        arms = _parse_arms(args.arms)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    output_root = args.output_root.expanduser().resolve()
    data_root = (REPO_ROOT / "data").resolve()
    if output_root == data_root or output_root.is_relative_to(data_root):
        raise SystemExit(f"refusing to write profiling output inside live data: {output_root}")
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + f"-{os.getpid()}"
    run_dir = output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    results_path = run_dir / "results.json"
    results: dict[str, Any] = {
        "schemaVersion": 1,
        "tool": "generation-cleanup-profile",
        "fixtureVersion": FIXTURE_VERSION,
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "cpuCount": psutil.cpu_count(),
            "pid": os.getpid(),
            "startedAt": _timestamp(),
        },
        "config": {
            "jobCount": args.job_count,
            "sourcesPerJob": args.sources_per_job,
            "deleteBatchSize": args.delete_batch_size,
            "largePayloadBytes": args.large_payload_bytes,
            "arms": list(arms),
            "keepFixtures": bool(args.keep_fixtures),
        },
        "builds": {},
        "arms": {},
        "summaries": {},
    }
    _write_json(results_path, results)
    needs_baseline = "baseline" in arms or "no-auto-checkpoint" in arms
    needs_large = "large-payload" in arms
    expected_deleted_rows = args.job_count + args.job_count * args.sources_per_job

    try:
        if needs_baseline:
            results["builds"]["baseline-source"] = build_database(
                run_dir / "fixtures" / "baseline-source",
                job_count=args.job_count,
                sources_per_job=args.sources_per_job,
                extra_payload_bytes=0,
            )
            _write_json(results_path, results)
        if needs_large:
            results["builds"]["large-source"] = build_database(
                run_dir / "fixtures" / "large-source",
                job_count=args.job_count,
                sources_per_job=args.sources_per_job,
                extra_payload_bytes=args.large_payload_bytes,
            )
            _write_json(results_path, results)

        for arm_name in arms:
            build_key = "large-source" if arm_name == "large-payload" else "baseline-source"
            build = results["builds"][build_key]
            arm = run_arm(
                arm_name=arm_name,
                base_db=Path(build["databasePath"]),
                work_dir=run_dir / "arms" / arm_name,
                disable_auto_checkpoint=arm_name == "no-auto-checkpoint",
                delete_batch_size=args.delete_batch_size,
                expected_deleted_rows=expected_deleted_rows,
            )
            results["arms"][arm_name] = arm
            results["summaries"][arm_name] = summarize_arm(arm)
            _write_json(results_path, results)
    finally:
        results["finishedAt"] = _timestamp()
        if not args.keep_fixtures:
            shutil.rmtree(run_dir / "fixtures", ignore_errors=True)
            shutil.rmtree(run_dir / "arms", ignore_errors=True)
            results["fixturesRemoved"] = True
        results["resultsPath"] = str(results_path)
        _write_json(results_path, results)
        (run_dir / "SUMMARY.md").write_text(_summary_markdown(results), encoding="utf-8")
    print(json.dumps(results["summaries"], indent=2, sort_keys=True))
    print(f"results: {results_path}")
    print(f"summary: {run_dir / 'SUMMARY.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
