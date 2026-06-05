from __future__ import annotations

import json
from pathlib import Path

from src import source_registry_io as registry_io
from src.storage_metrics import (
    record_json_write,
    record_source_sync_snapshot,
    record_storage_read,
    registry_journal_telemetry,
    reset_storage_metrics,
    snapshot_storage_metrics,
)


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


def test_save_json_atomic_records_storage_metrics_without_recursing(tmp_path: Path) -> None:
    reset_storage_metrics(data_dir=tmp_path, remove_file=True)
    path = tmp_path / "jobs-fetch-report.json"

    registry_io.save_json_atomic(path, {"runId": "fetch_1", "status": "completed"})

    metrics = snapshot_storage_metrics(tmp_path)
    artifact_names = {str(row.get("artifact") or "") for row in metrics["writes"]["artifacts"]}

    assert "jobs-fetch-report.json" in artifact_names
    assert "storage-metrics.jsonl" not in artifact_names
    assert json.loads(path.read_text(encoding="utf-8"))["runId"] == "fetch_1"
