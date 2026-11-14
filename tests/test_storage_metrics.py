from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import cast

from src import source_registry_io as registry_io
from src import storage_metrics as storage_metrics_mod
from src.storage import BaluffoStore
from src.storage_metrics import (
    record_json_write,
    record_jsonl_write,
    record_source_sync_snapshot,
    record_storage_read,
    registry_journal_telemetry,
    reset_storage_metrics,
    snapshot_storage_metrics,
)
from tests.helpers.temp_paths import workspace_tmpdir
from tests.storage.test_baluffo_store_write_path import _slow_store_write_rows


def test_storage_metrics_records_json_write_stats(tmp_path: Path) -> None:
    reset_storage_metrics(data_dir=tmp_path, remove_file=True)

    record_json_write(
        path=tmp_path / "jobs-fetch-report.json",
        target=tmp_path / "jobs-fetch-report.json",
        storage_kind="json",
        serialization_duration_ms=2,
        atomic_replace_duration_ms=4,
        compressed_size_bytes=100,
        uncompressed_size_bytes=120,
        replaced=True,
        data_dir=tmp_path,
    )
    record_json_write(
        path=tmp_path / "jobs-fetch-report.json",
        target=tmp_path / "jobs-fetch-report.json",
        storage_kind="json",
        serialization_duration_ms=6,
        atomic_replace_duration_ms=8,
        compressed_size_bytes=300,
        uncompressed_size_bytes=360,
        replaced=True,
        data_dir=tmp_path,
    )

    metrics = snapshot_storage_metrics(tmp_path)
    writes = metrics["writes"]

    assert writes["writeCount"] == 2
    assert writes["totals"]["serializationDurationMs"]["median"] == 4
    assert writes["totals"]["compressedSizeBytes"]["max"] == 300
    assert writes["artifacts"][0]["artifact"] == "jobs-fetch-report.json"
    assert (tmp_path / "storage-metrics.jsonl").exists()


def test_storage_metrics_jsonl_rotates_when_file_is_oversized(tmp_path: Path, monkeypatch) -> None:
    reset_storage_metrics(data_dir=tmp_path, remove_file=True)
    monkeypatch.setattr(storage_metrics_mod, "MAX_METRICS_FILE_BYTES", 128)
    path = tmp_path / "storage-metrics.jsonl"
    path.write_text("x" * 256, encoding="utf-8")

    record_jsonl_write(
        path=tmp_path / "jobs-fetch-report.jsonl",
        operation="append",
        bytes_written=10,
        duration_ms=1,
        data_dir=tmp_path,
    )

    assert (tmp_path / "storage-metrics.jsonl.1").exists()
    assert (tmp_path / "storage-metrics.jsonl.1").stat().st_size == 256
    assert not path.read_text(encoding="utf-8").startswith("x")


def test_storage_metrics_snapshot_reads_only_tail_bytes(tmp_path: Path, monkeypatch) -> None:
    reset_storage_metrics(data_dir=tmp_path, remove_file=True)
    monkeypatch.setattr(storage_metrics_mod, "MAX_EVENT_READ_BYTES", 256)
    path = tmp_path / "storage-metrics.jsonl"
    old_rows = [
        json.dumps(
            {
                "schemaVersion": 1,
                "eventId": f"old-{idx}",
                "type": "jsonWrite",
                "artifact": f"old-{idx}.json",
                "padding": "x" * 256,
                "writeCount": 1,
            }
        )
        for idx in range(100)
    ]
    tail_row = json.dumps(
        {
            "schemaVersion": 1,
            "eventId": "tail",
            "type": "jsonWrite",
            "artifact": "tail.json",
            "writeCount": 1,
        }
    )
    path.write_text("\n".join(old_rows + [tail_row]) + "\n", encoding="utf-8")

    metrics = snapshot_storage_metrics(tmp_path)
    artifact_names = {str(row.get("artifact") or "") for row in metrics["writes"]["artifacts"]}

    assert "tail.json" in artifact_names
    assert "old-0.json" not in artifact_names
    assert metrics["metricsFile"]["tailReadBytes"] == 256


def test_storage_metrics_records_bounded_read_stats(tmp_path: Path) -> None:
    reset_storage_metrics(data_dir=tmp_path, remove_file=True)

    record_storage_read(
        surface="registry.summary",
        artifact=tmp_path / "source-registry-active.json",
        storage_kind="sqlite",
        duration_ms=5,
        bytes_read=100,
        row_count=3,
        data_dir=tmp_path,
    )
    record_storage_read(
        surface="../unsafe/registry.summary",
        artifact=r"C:\secret\registry.json",
        storage_kind="json",
        duration_ms=7,
        failed=True,
        memory_delta_bytes=-50,
        data_dir=tmp_path,
    )

    metrics = snapshot_storage_metrics(tmp_path)
    reads = metrics["reads"]

    assert reads["readCount"] == 2
    assert reads["failedReadCount"] == 1
    assert reads["totals"]["durationMs"]["max"] == 7
    assert reads["totals"]["bytesRead"]["total"] == 100
    assert reads["totals"]["memoryDeltaBytes"]["min"] == -50
    labels = {(row["surface"], row["artifact"]) for row in reads["surfaces"]}
    assert ("registry.summary", "source-registry-active.json") in labels
    assert all("secret" not in artifact.lower() for _surface, artifact in labels)


def test_registry_journal_telemetry_counts_rows_and_bytes(tmp_path: Path) -> None:
    journal_path = tmp_path / "source-registry-active.jsonl"
    journal_path.write_text('{"payload":[]}\n{"payload":[{"id":"one"}]}\n', encoding="utf-8")

    telemetry = registry_journal_telemetry(tmp_path, compact_threshold_bytes=1)

    assert telemetry["registryJsonlJournalBytes"] == journal_path.stat().st_size
    assert telemetry["registryJsonlJournalRows"] == 2
    assert telemetry["files"][0]["name"] == "source-registry-active.jsonl"
    assert telemetry["files"][0]["compactThresholdExceeded"] is True


def test_source_sync_snapshot_metrics_are_visible(tmp_path: Path) -> None:
    reset_storage_metrics(data_dir=tmp_path, remove_file=True)

    record_source_sync_snapshot(
        size_bytes=1234,
        max_snapshot_size_bytes=10_000,
        size_warning=False,
        would_change=True,
        snapshot_format="sharded-v3",
        shard_count=3,
        changed_shard_count=2,
        shards_pushed_bytes=2048,
        manifest_size_bytes=512,
        shard_cap_bytes=10 * 1024 * 1024,
        shard_hashes={"shard-path": "sha"},
        data_dir=tmp_path,
    )

    metrics = snapshot_storage_metrics(tmp_path)

    assert metrics["sourceSyncSnapshots"]["snapshotCount"] == 1
    assert metrics["sourceSyncSnapshots"]["latestSizeBytes"] == 1234
    assert metrics["sourceSyncSnapshots"]["maxSnapshotSizeBytes"] == 10_000
    assert metrics["sourceSyncSnapshots"]["snapshotFormat"] == "sharded-v3"
    assert metrics["sourceSyncSnapshots"]["shardCount"] == 3
    assert metrics["sourceSyncSnapshots"]["changedShardCount"] == 2
    assert metrics["sourceSyncSnapshots"]["shardsPushedBytes"] == 2048
    assert metrics["sourceSyncSnapshots"]["manifestSizeBytes"] == 512
    assert metrics["sourceSyncSnapshots"]["shardCapBytes"] == 10 * 1024 * 1024
    assert metrics["sourceSyncSnapshots"]["shardHashes"] == {"shard-path": "sha"}


def test_slow_store_write_metrics_name_the_waiter_and_the_holder(tmp_path: Path) -> None:
    reset_storage_metrics(data_dir=tmp_path, remove_file=True)

    storage_metrics_mod.record_slow_store_write(
        operation="write",
        wait_ms=12_500,
        hold_ms=4,
        thread_name="baluffo-bridge-on-started",
        stack='  File "src/bridge/admin_task_runtime.py", line 298, in <lambda>\n',
        data_dir=tmp_path,
    )
    storage_metrics_mod.record_slow_store_write(
        operation="vacuum",
        wait_ms=0,
        hold_ms=13_200,
        thread_name="baluffo-sqlite-post-serve-maintenance",
        stack='  File "src/storage/baluffo_store.py", line 769, in vacuum\n',
        data_dir=tmp_path,
    )
    reset_storage_metrics(data_dir=tmp_path, remove_file=False)

    slow = snapshot_storage_metrics(tmp_path)["slowWrites"]

    assert slow["count"] == 2
    assert slow["maxWaitMs"] == 12_500
    assert slow["maxHoldMs"] == 13_200
    assert [row["operation"] for row in slow["operations"]] == ["write", "vacuum"]
    assert slow["operations"][0]["lastThread"] == "baluffo-bridge-on-started"
    assert slow["recent"][0]["lastFrame"].endswith("in <lambda>")
    assert slow["recent"][1]["thread"] == "baluffo-sqlite-post-serve-maintenance"


def test_slow_store_write_timings_are_summarized_per_operation(tmp_path: Path) -> None:
    reset_storage_metrics(data_dir=tmp_path, remove_file=True)

    storage_metrics_mod.record_slow_store_write(
        operation="cleanup-delete:job_sources",
        wait_ms=0,
        hold_ms=4_500,
        thread_name="baluffo-sqlite-runtime-retention",
        stack='  File "src/storage/job_runtime.py", line 780, in delete_rows\n',
        data_dir=tmp_path,
        timings={
            "callbackMs": 23,
            "deleteMs": 23,
            "commitMs": 4_470,
            "walBytesBefore": 78_000_000,
            "walBytesAfter": 79_000_000,
            "walGrowthBytes": 1_000_000,
        },
    )
    storage_metrics_mod.record_slow_store_write(
        operation="checkpoint:truncate",
        wait_ms=0,
        hold_ms=600,
        thread_name="baluffo-sqlite-runtime-retention",
        data_dir=tmp_path,
        timings={
            "checkpointMs": 600,
            "walBytesBefore": 79_000_000,
            "walBytesAfter": 0,
            "walGrowthBytes": -79_000_000,
        },
    )
    reset_storage_metrics(data_dir=tmp_path, remove_file=False)

    slow = snapshot_storage_metrics(tmp_path)["slowWrites"]
    cleanup = next(
        row for row in slow["operations"] if row["operation"] == "cleanup-delete:job_sources"
    )

    assert slow["timings"]["deleteMs"]["max"] == 23
    assert slow["timings"]["commitMs"]["max"] == 4_470
    assert slow["timings"]["checkpointMs"]["max"] == 600
    assert slow["timings"]["walGrowthBytes"]["min"] == -79_000_000
    assert cleanup["timings"]["commitMs"]["max"] == 4_470
    assert cleanup["timings"]["deleteMs"]["max"] == 23
    assert slow["recent"][0]["timings"]["deleteMs"] == 23
    assert slow["recent"][1]["timings"]["checkpointMs"] == 600
    assert "commitMs" not in slow["recent"][1]["timings"]


def test_slow_cleanup_delete_records_delete_commit_and_wal_timings() -> None:
    with workspace_tmpdir("baluffo-store-slow-delete-timings") as data_dir:
        with BaluffoStore(data_dir, slow_write_warn_ms=1) as store:
            store.write(
                lambda conn: conn.execute(
                    "CREATE TABLE delete_timing_probe (id INTEGER PRIMARY KEY, payload TEXT)"
                )
            )

            def slow_delete(conn: sqlite3.Connection) -> int:
                time.sleep(0.02)
                cursor = conn.execute(
                    "INSERT INTO delete_timing_probe(payload) VALUES (?)", ("x" * 4096,)
                )
                return int(cursor.rowcount or 0)

            store.write(slow_delete, operation="cleanup-delete:jobs")

        rows = [
            row
            for row in _slow_store_write_rows(data_dir)
            if row.get("operation") == "cleanup-delete:jobs"
        ]

        assert rows, "a slow cleanup delete must be journaled with its operation label"
        row = rows[-1]
        assert row["deleteMs"] == row["callbackMs"]
        assert int(cast(int, row["deleteMs"])) >= 15
        assert int(cast(int, row["commitMs"])) >= 0
        assert int(cast(int, row["walBytesAfter"])) >= int(cast(int, row["walBytesBefore"]))
        assert int(cast(int, row["walGrowthBytes"])) == int(cast(int, row["walBytesAfter"])) - int(
            cast(int, row["walBytesBefore"])
        )


def test_slow_checkpoint_records_checkpoint_and_wal_timings() -> None:
    with workspace_tmpdir("baluffo-store-slow-checkpoint-timings") as data_dir:
        with BaluffoStore(data_dir, slow_write_warn_ms=1) as store:
            store.write(
                lambda conn: conn.execute(
                    "CREATE TABLE checkpoint_timing_probe (id INTEGER PRIMARY KEY, payload TEXT)"
                )
            )
            store.write(
                lambda conn: conn.execute(
                    "INSERT INTO checkpoint_timing_probe(payload) VALUES (?)", ("x" * 4096,)
                )
            )
            held = threading.Event()
            release = threading.Event()

            def hold_write_lock() -> None:
                with store._write_lock_guard("test-checkpoint-wait"):
                    held.set()
                    release.wait(5)

            holder = threading.Thread(target=hold_write_lock, daemon=True)
            holder.start()
            assert held.wait(2)
            try:
                store.checkpoint_required(mode="TRUNCATE")
            finally:
                release.set()
                holder.join(timeout=5)

            rows = [
                row
                for row in _slow_store_write_rows(data_dir)
                if row.get("operation") == "checkpoint:truncate"
            ]

            assert rows, "a checkpoint that waited past the threshold must be journaled"
            row = rows[-1]
            assert int(cast(int, row["checkpointMs"])) >= 0
            assert int(cast(int, row["walBytesBefore"])) > 0
            assert int(cast(int, row["walBytesAfter"])) == 0
            assert int(cast(int, row["walGrowthBytes"])) == int(
                cast(int, row["walBytesAfter"])
            ) - int(cast(int, row["walBytesBefore"]))


def test_save_json_atomic_records_storage_metrics_without_recursing(tmp_path: Path) -> None:
    reset_storage_metrics(data_dir=tmp_path, remove_file=True)
    path = tmp_path / "jobs-fetch-report.json"

    registry_io.save_json_atomic(path, {"runId": "fetch_1", "status": "completed"})

    metrics = snapshot_storage_metrics(tmp_path)
    artifact_names = {str(row.get("artifact") or "") for row in metrics["writes"]["artifacts"]}

    assert "jobs-fetch-report.json" in artifact_names
    assert "storage-metrics.jsonl" not in artifact_names
    assert json.loads(path.read_text(encoding="utf-8"))["runId"] == "fetch_1"
