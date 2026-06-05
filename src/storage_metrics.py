from __future__ import annotations

import json
import os
import re
import threading
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STORAGE_METRICS_FILE_NAME = "storage-metrics.jsonl"
STORAGE_METRICS_SCHEMA_VERSION = 1
DEFAULT_COMPACT_THRESHOLD_BYTES = 1_048_576
MAX_IN_MEMORY_EVENTS = 2_000
MAX_EVENT_READ_ROWS = 5_000
REGISTRY_JOURNAL_NAMES = (
    "source-registry-active.jsonl",
    "source-registry-pending.jsonl",
    "source-registry-rejected.jsonl",
    "source-registry-tombstones.jsonl",
)
MAX_READ_LABEL_LENGTH = 96
_SAFE_READ_LABEL_RE = re.compile(r"[^a-zA-Z0-9_.:-]+")

_LOCK = threading.RLock()
_EVENTS: list[dict[str, Any]] = []
_EVENT_COUNTER = 0


def _default_data_dir() -> Path:
    raw = str(os.environ.get("BALUFFO_DATA_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (Path(__file__).resolve().parents[1] / "data").resolve()


def _metrics_path(data_dir: Path | str | None = None) -> Path:
    root = Path(data_dir).expanduser().resolve() if data_dir is not None else _default_data_dir()
    return root / STORAGE_METRICS_FILE_NAME


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def duration_ms(started_at: float) -> int:
    return max(0, int(round((time.perf_counter() - started_at) * 1000)))


def _next_event_id() -> str:
    global _EVENT_COUNTER
    with _LOCK:
        _EVENT_COUNTER += 1
        return f"{os.getpid()}-{time.time_ns()}-{_EVENT_COUNTER}"


def _append_event(event: dict[str, Any], *, data_dir: Path | str | None = None) -> None:
    row = {
        "schemaVersion": STORAGE_METRICS_SCHEMA_VERSION,
        "eventId": _next_event_id(),
        "ts": _now_iso(),
        "pid": os.getpid(),
        **event,
    }
    with _LOCK:
        _EVENTS.append(row)
        del _EVENTS[:-MAX_IN_MEMORY_EVENTS]
    try:
        path = _metrics_path(data_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    except OSError:
        # Diagnostics must never break runtime writes.
        return


def record_json_write(
    *,
    path: Path | str,
    target: Path | str,
    storage_kind: str,
    serialization_duration_ms: int,
    atomic_replace_duration_ms: int,
    compressed_size_bytes: int,
    uncompressed_size_bytes: int,
    replaced: bool,
    data_dir: Path | str | None = None,
) -> None:
    target_path = Path(target)
    _append_event(
        {
            "type": "jsonWrite",
            "artifact": Path(path).name,
            "path": str(Path(path)),
            "target": str(target_path),
            "storageKind": str(storage_kind or "json"),
            "serializationDurationMs": max(0, int(serialization_duration_ms or 0)),
            "atomicReplaceDurationMs": max(0, int(atomic_replace_duration_ms or 0)),
            "compressedSizeBytes": max(0, int(compressed_size_bytes or 0)),
            "uncompressedSizeBytes": max(0, int(uncompressed_size_bytes or 0)),
            "writeCount": 1,
            "replaced": bool(replaced),
        },
        data_dir=data_dir,
    )


def record_jsonl_write(
    *,
    path: Path | str,
    operation: str,
    bytes_written: int,
    duration_ms: int,
    row_count: int = 1,
    replaced: bool = True,
    data_dir: Path | str | None = None,
) -> None:
    target_path = Path(path)
    _append_event(
        {
            "type": "jsonlWrite",
            "artifact": target_path.name,
            "path": str(target_path),
            "target": str(target_path),
            "storageKind": "jsonl",
            "operation": str(operation or "write"),
            "serializationDurationMs": 0,
            "atomicReplaceDurationMs": max(0, int(duration_ms or 0)),
            "compressedSizeBytes": max(0, int(bytes_written or 0)),
            "uncompressedSizeBytes": max(0, int(bytes_written or 0)),
            "rowCount": max(0, int(row_count or 0)),
            "writeCount": 1,
            "replaced": bool(replaced),
        },
        data_dir=data_dir,
    )


def record_source_sync_snapshot(
    *,
    size_bytes: int,
    max_snapshot_size_bytes: int,
    size_warning: bool,
    would_change: bool,
    snapshot_format: str = "",
    shard_count: int = 0,
    changed_shard_count: int = 0,
    shards_pushed_bytes: int = 0,
    manifest_size_bytes: int = 0,
    shard_cap_bytes: int = 0,
    shard_hashes: dict[str, Any] | None = None,
    data_dir: Path | str | None = None,
) -> None:
    _append_event(
        {
            "type": "sourceSyncSnapshot",
            "artifact": "source-sync",
            "sizeBytes": max(0, int(size_bytes or 0)),
            "maxSnapshotSizeBytes": max(0, int(max_snapshot_size_bytes or 0)),
            "sizeWarning": bool(size_warning),
            "wouldChange": bool(would_change),
            "snapshotFormat": str(snapshot_format or ""),
            "shardCount": max(0, int(shard_count or 0)),
            "changedShardCount": max(0, int(changed_shard_count or 0)),
            "shardsPushedBytes": max(0, int(shards_pushed_bytes or 0)),
            "manifestSizeBytes": max(0, int(manifest_size_bytes or 0)),
            "shardCapBytes": max(0, int(shard_cap_bytes or 0)),
            "shardHashes": dict(shard_hashes or {}),
        },
        data_dir=data_dir,
    )


def reset_storage_metrics(*, data_dir: Path | str | None = None, remove_file: bool = False) -> None:
    global _EVENT_COUNTER
    with _LOCK:
        _EVENTS.clear()
        _EVENT_COUNTER = 0
    if remove_file:
        try:
            _metrics_path(data_dir).unlink()
        except FileNotFoundError:
            return
        except OSError:
            return


def _read_metric_events(data_dir: Path | str | None = None) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    path = _metrics_path(data_dir)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        lines = []
    for line in lines[-MAX_EVENT_READ_ROWS:]:
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            events.append(payload)
    seen = {str(row.get("eventId") or "") for row in events}
    with _LOCK:
        memory_events = list(_EVENTS)
    for row in memory_events:
        event_id = str(row.get("eventId") or "")
        if event_id and event_id not in seen:
            events.append(row)
    return events[-MAX_EVENT_READ_ROWS:]


def _int_value(value: Any) -> int:
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return 0


def _signed_int_value(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _safe_read_label(value: Any, *, fallback: str) -> str:
    if isinstance(value, Path):
        text = value.name
    else:
        text = str(value or "").strip()
        if "\\" in text or "/" in text:
            text = text.replace("\\", "/").rsplit("/", 1)[-1]
    text = _SAFE_READ_LABEL_RE.sub("_", text).strip("_.:-")
    if not text:
        text = fallback
    if len(text) > MAX_READ_LABEL_LENGTH:
        text = f"{text[: MAX_READ_LABEL_LENGTH - 1].rstrip('_.:-')}~"
    return text or fallback


def record_storage_read(
    *,
    surface: str,
    artifact: str | Path = "",
    storage_kind: str = "unknown",
    duration_ms: int = 0,
    bytes_read: int = 0,
    row_count: int = 0,
    failed: bool = False,
    memory_delta_bytes: int = 0,
    data_dir: Path | str | None = None,
) -> None:
    """Record bounded aggregate evidence for runtime storage reads.

    Read metrics intentionally keep only stable labels and counters; callers must not
    pass payload bodies, query strings, arbitrary paths, or dynamic IDs.
    """

    _append_event(
        {
            "type": "storageRead",
            "surface": _safe_read_label(surface, fallback="unknown"),
            "artifact": _safe_read_label(artifact, fallback="unknown"),
            "storageKind": _safe_read_label(storage_kind, fallback="unknown"),
            "durationMs": max(0, int(duration_ms or 0)),
            "bytesRead": max(0, int(bytes_read or 0)),
            "rowCount": max(0, int(row_count or 0)),
            "failed": bool(failed),
            "memoryDeltaBytes": _signed_int_value(memory_delta_bytes),
            "readCount": 1,
        },
        data_dir=data_dir,
    )


def _median(values: list[int]) -> int:
    ordered = sorted(values)
    if not ordered:
        return 0
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return int(round((ordered[middle - 1] + ordered[middle]) / 2))


def _stats(values: Iterable[Any]) -> dict[str, int]:
    parsed = [_int_value(value) for value in values]
    parsed = [value for value in parsed if value >= 0]
    return {
        "count": len(parsed),
        "min": min(parsed) if parsed else 0,
        "median": _median(parsed),
        "max": max(parsed) if parsed else 0,
        "total": sum(parsed),
    }


def _signed_stats(values: Iterable[Any]) -> dict[str, int]:
    parsed = [_signed_int_value(value) for value in values]
    return {
        "count": len(parsed),
        "min": min(parsed) if parsed else 0,
        "median": _median(parsed),
        "max": max(parsed) if parsed else 0,
        "total": sum(parsed),
    }


def _summarize_write_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in events:
        if row.get("type") not in {"jsonWrite", "jsonlWrite"}:
            continue
        key = str(row.get("artifact") or row.get("target") or "unknown")
        grouped.setdefault(key, []).append(row)

    artifacts: list[dict[str, Any]] = []
    for artifact, rows in sorted(grouped.items()):
        latest = rows[-1]
        artifacts.append(
            {
                "artifact": artifact,
                "path": str(latest.get("path") or ""),
                "target": str(latest.get("target") or ""),
                "storageKind": str(latest.get("storageKind") or ""),
                "writeCount": sum(_int_value(row.get("writeCount")) or 1 for row in rows),
                "failedWriteCount": sum(1 for row in rows if not bool(row.get("replaced", True))),
                "serializationDurationMs": _stats(
                    row.get("serializationDurationMs") for row in rows
                ),
                "atomicReplaceDurationMs": _stats(
                    row.get("atomicReplaceDurationMs") for row in rows
                ),
                "compressedSizeBytes": _stats(row.get("compressedSizeBytes") for row in rows),
                "uncompressedSizeBytes": _stats(row.get("uncompressedSizeBytes") for row in rows),
                "lastCompressedSizeBytes": _int_value(latest.get("compressedSizeBytes")),
                "lastUncompressedSizeBytes": _int_value(latest.get("uncompressedSizeBytes")),
            }
        )

    return {
        "artifactCount": len(artifacts),
        "writeCount": sum(
            _int_value(row.get("writeCount")) or 1
            for row in events
            if row.get("type") in {"jsonWrite", "jsonlWrite"}
        ),
        "artifacts": artifacts,
        "totals": {
            "serializationDurationMs": _stats(
                row.get("serializationDurationMs")
                for row in events
                if row.get("type") in {"jsonWrite", "jsonlWrite"}
            ),
            "atomicReplaceDurationMs": _stats(
                row.get("atomicReplaceDurationMs")
                for row in events
                if row.get("type") in {"jsonWrite", "jsonlWrite"}
            ),
            "compressedSizeBytes": _stats(
                row.get("compressedSizeBytes")
                for row in events
                if row.get("type") in {"jsonWrite", "jsonlWrite"}
            ),
            "uncompressedSizeBytes": _stats(
                row.get("uncompressedSizeBytes")
                for row in events
                if row.get("type") in {"jsonWrite", "jsonlWrite"}
            ),
        },
    }


def _summarize_read_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in events if row.get("type") == "storageRead"]
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        surface = _safe_read_label(row.get("surface"), fallback="unknown")
        artifact = _safe_read_label(row.get("artifact"), fallback="unknown")
        storage_kind = _safe_read_label(row.get("storageKind"), fallback="unknown")
        grouped.setdefault((surface, artifact, storage_kind), []).append(row)

    surfaces: list[dict[str, Any]] = []
    for (surface, artifact, storage_kind), group_rows in sorted(grouped.items()):
        surfaces.append(
            {
                "surface": surface,
                "artifact": artifact,
                "storageKind": storage_kind,
                "readCount": sum(_int_value(row.get("readCount")) or 1 for row in group_rows),
                "failedReadCount": sum(1 for row in group_rows if bool(row.get("failed"))),
                "durationMs": _stats(row.get("durationMs") for row in group_rows),
                "bytesRead": _stats(row.get("bytesRead") for row in group_rows),
                "rowCount": _stats(row.get("rowCount") for row in group_rows),
                "memoryDeltaBytes": _signed_stats(
                    row.get("memoryDeltaBytes") for row in group_rows
                ),
            }
        )

    surfaces.sort(
        key=lambda row: (
            int(row["durationMs"].get("max") or 0),
            int(row["bytesRead"].get("max") or 0),
            int(row.get("readCount") or 0),
        ),
        reverse=True,
    )
    return {
        "surfaceCount": len(surfaces),
        "readCount": sum(_int_value(row.get("readCount")) or 1 for row in rows),
        "failedReadCount": sum(1 for row in rows if bool(row.get("failed"))),
        "surfaces": surfaces,
        "totals": {
            "durationMs": _stats(row.get("durationMs") for row in rows),
            "bytesRead": _stats(row.get("bytesRead") for row in rows),
            "rowCount": _stats(row.get("rowCount") for row in rows),
            "memoryDeltaBytes": _signed_stats(row.get("memoryDeltaBytes") for row in rows),
        },
    }


def _summarize_source_sync(events: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in events if row.get("type") == "sourceSyncSnapshot"]
    latest = rows[-1] if rows else {}
    return {
        "snapshotCount": len(rows),
        "sizeBytes": _stats(row.get("sizeBytes") for row in rows),
        "maxSnapshotSizeBytes": _int_value(latest.get("maxSnapshotSizeBytes")),
        "latestSizeBytes": _int_value(latest.get("sizeBytes")),
        "latestSizeWarning": bool(latest.get("sizeWarning")) if latest else False,
        "latestWouldChange": bool(latest.get("wouldChange")) if latest else False,
        "snapshotFormat": str(latest.get("snapshotFormat") or ""),
        "shardCount": _int_value(latest.get("shardCount")),
        "changedShardCount": _int_value(latest.get("changedShardCount")),
        "shardsPushedBytes": _int_value(latest.get("shardsPushedBytes")),
        "manifestSizeBytes": _int_value(latest.get("manifestSizeBytes")),
        "shardCapBytes": _int_value(latest.get("shardCapBytes")),
        "shardHashes": dict(latest.get("shardHashes") or {}),
    }


def registry_journal_telemetry(
    data_dir: Path | str | None = None,
    *,
    compact_threshold_bytes: int = DEFAULT_COMPACT_THRESHOLD_BYTES,
    journal_names: Iterable[str] = REGISTRY_JOURNAL_NAMES,
) -> dict[str, Any]:
    root = Path(data_dir).expanduser().resolve() if data_dir is not None else _default_data_dir()
    files: list[dict[str, Any]] = []
    total_bytes = 0
    total_rows = 0
    for name in sorted({str(item) for item in journal_names if str(item or "").strip()}):
        path = root / name
        try:
            byte_size = path.stat().st_size
        except OSError:
            byte_size = 0
        row_count = 0
        if byte_size > 0:
            try:
                with path.open("r", encoding="utf-8") as handle:
                    row_count = sum(1 for line in handle if line.strip())
            except OSError:
                row_count = 0
        total_bytes += byte_size
        total_rows += row_count
        if byte_size > 0 or row_count > 0:
            files.append(
                {
                    "path": str(path),
                    "name": name,
                    "byteSize": byte_size,
                    "rowCount": row_count,
                    "compactThresholdExceeded": byte_size > compact_threshold_bytes,
                }
            )
    return {
        "registryJsonlJournalBytes": total_bytes,
        "registryJsonlJournalRows": total_rows,
        "compactThresholdBytes": compact_threshold_bytes,
        "files": files,
    }


def snapshot_storage_metrics(data_dir: Path | str | None = None) -> dict[str, Any]:
    root = Path(data_dir).expanduser().resolve() if data_dir is not None else _default_data_dir()
    events = _read_metric_events(root)
    return {
        "schemaVersion": STORAGE_METRICS_SCHEMA_VERSION,
        "generatedAt": _now_iso(),
        "dataDir": str(root),
        "eventCount": len(events),
        "writes": _summarize_write_events(events),
        "reads": _summarize_read_events(events),
        "registryJournals": registry_journal_telemetry(root),
        "sourceSyncSnapshots": _summarize_source_sync(events),
    }
