"""Source registry JSON journal subsystem.

AI boundary owns: journal maintenance (quarantine cleanup, compaction), journal record
builders (image/hash/delta), and append/compact writes.
AI boundary implement in: this leaf for the journal; path helpers come from
``source_registry_io_paths.py``. Seam: loaders and raw readers are resolved through the
coordinator at call time to keep the module-level DAG acyclic, and DATA_DIR resolves
through the coordinator so the runtime rebind is observed.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.source_registry_io_paths import (
    _JSON_JOURNAL_DELTA_SCHEMA_VERSION,
    _JSON_JOURNAL_SCHEMA_VERSION,
    _REGISTRY_JOURNAL_FILE_NAMES,
    _RUNTIME_EVIDENCE_FILE_NAMES,
    _RUNTIME_EVIDENCE_JOURNAL_QUARANTINE_DIR,
    _WRITE_POLICY_REQUIRED,
    _is_runtime_evidence_file,
    _run_write_with_retries,
    _storage_base_name,
    _storage_metrics_data_dir_for,
    _uses_json_journal,
    ensure_data_dir,
)
from src.storage_metrics import duration_ms, record_jsonl_write


def _json_journal_path_for(path: Path) -> Path:
    base_name = _storage_base_name(Path(path))
    if base_name.endswith(".json"):
        return Path(path).with_name(f"{base_name[:-5]}.jsonl")
    return Path(path).with_name(f"{base_name}.jsonl")


def _runtime_evidence_journal_paths(data_dir: Path) -> list[Path]:
    return [
        _json_journal_path_for(Path(data_dir) / file_name)
        for file_name in sorted(_RUNTIME_EVIDENCE_FILE_NAMES)
    ]


def _unique_quarantine_path(quarantine_dir: Path, journal_path: Path) -> Path:
    stem = journal_path.stem
    suffix = journal_path.suffix
    for attempt in range(100):
        target = quarantine_dir / f"{stem}.{time.time_ns()}.{attempt}{suffix}"
        if not target.exists():
            return target
    return quarantine_dir / f"{stem}.{os.getpid()}.{time.time_ns()}{suffix}"


def cleanup_runtime_evidence_journals(data_dir: Path | None = None) -> dict[str, Any]:
    """Quarantine stale journals that must not participate in runtime evidence reads."""

    from src import source_registry_io as _srio

    root = Path(data_dir) if data_dir is not None else _srio.DATA_DIR
    result: dict[str, Any] = {
        "checked": 0,
        "quarantined": [],
        "errors": [],
    }
    quarantine_dir = root / _RUNTIME_EVIDENCE_JOURNAL_QUARANTINE_DIR
    for journal_path in _runtime_evidence_journal_paths(root):
        result["checked"] += 1
        if not journal_path.exists():
            continue
        try:
            byte_size = journal_path.stat().st_size
            quarantine_dir.mkdir(parents=True, exist_ok=True)
            target = _unique_quarantine_path(quarantine_dir, journal_path)
            journal_path.replace(target)
            result["quarantined"].append(
                {
                    "path": str(journal_path),
                    "quarantinePath": str(target),
                    "byteSize": byte_size,
                }
            )
        except OSError as exc:
            result["errors"].append({"path": str(journal_path), "error": str(exc)})
    result["ok"] = not result["errors"]
    return result


def _registry_journal_expects_array(path: Path) -> bool:
    return _storage_base_name(path) != "source-registry-tombstones.json"


def _registry_journal_repair_payload(path: Path) -> Any:
    from src import source_registry_io as _srio

    latest_payload = _srio._load_json_journal_latest_payload(path)
    if _registry_journal_expects_array(path):
        if isinstance(latest_payload, list):
            return [dict(row) for row in latest_payload if isinstance(row, dict)]
        return _srio.load_json_array(path, [])
    if isinstance(latest_payload, dict):
        return dict(latest_payload)
    return _srio.load_json_object(path, {})


def compact_registry_journals(data_dir: Path | None = None) -> dict[str, Any]:
    from src import source_registry_io as _srio

    root = Path(data_dir or _srio.DATA_DIR)
    result: dict[str, Any] = {
        "ok": True,
        "checked": 0,
        "compacted": [],
        "skipped": [],
        "errors": [],
    }
    for file_name in sorted(_REGISTRY_JOURNAL_FILE_NAMES):
        path = root / file_name
        journal_path = _json_journal_path_for(path)
        result["checked"] += 1
        try:
            if not journal_path.exists():
                result["skipped"].append({"path": str(journal_path), "reason": "missing"})
                continue
            byte_size = journal_path.stat().st_size
            if byte_size <= _srio._JSON_JOURNAL_COMPACT_MAX_BYTES:
                result["skipped"].append(
                    {
                        "path": str(journal_path),
                        "reason": "below_threshold",
                        "byteSize": byte_size,
                    }
                )
                continue
            payload = _srio._registry_journal_repair_payload(path)
            _srio._write_text_atomic(
                journal_path,
                _registry_journal_record_text(path, payload),
                policy=_WRITE_POLICY_REQUIRED,
            )
            compacted_size = journal_path.stat().st_size
            result["compacted"].append(
                {
                    "path": str(journal_path),
                    "byteSizeBefore": byte_size,
                    "byteSizeAfter": compacted_size,
                }
            )
        except (OSError, TypeError, ValueError) as exc:
            result["ok"] = False
            result["errors"].append(
                {
                    "path": str(journal_path),
                    "errorType": type(exc).__name__,
                    "error": str(exc),
                }
            )
    return result


def _path_mtime_ns(path: Path) -> int | None:
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return None


def _json_journal_should_overlay_base(path: Path, existing: Path | None) -> bool:
    if existing is None:
        return True
    journal_mtime = _path_mtime_ns(_json_journal_path_for(path))
    if journal_mtime is None:
        return False
    existing_mtime = _path_mtime_ns(existing)
    if existing_mtime is None:
        return True
    return journal_mtime > existing_mtime


def _json_journal_image_payload(payload: Any) -> Any:
    if isinstance(payload, list):
        return [dict(row) if isinstance(row, dict) else row for row in payload]
    if isinstance(payload, dict):
        return dict(payload)
    return payload


def _json_journal_payload_hash(payload: Any) -> str:
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _json_journal_record(payload: Any) -> dict[str, Any]:
    image_payload = _json_journal_image_payload(payload)
    return {
        "schemaVersion": _JSON_JOURNAL_SCHEMA_VERSION,
        "contentHash": _json_journal_payload_hash(image_payload),
        "payload": image_payload,
    }


def _json_journal_record_text(payload: Any) -> str:
    return (
        json.dumps(
            _json_journal_record(payload),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n"
    )


def _json_journal_timestamp() -> str:
    return datetime.now(UTC).isoformat()


def _registry_journal_base_payload(path: Path) -> Any:
    from src import source_registry_io as _srio

    if _registry_journal_expects_array(path):
        rows = _srio._load_json_array_from_storage(path, [])
        return rows if rows is not None else []
    return _srio._load_json_object_from_storage(path, {})


def _registry_rows_by_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]] | None:
    rows_by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_id = row.get("id")
        if not isinstance(row_id, str) or not row_id or row_id in rows_by_id:
            return None
        rows_by_id[row_id] = dict(row)
    return rows_by_id


def _json_journal_array_delta_record(
    base_payload: Any,
    current_payload: Any,
) -> dict[str, Any] | None:
    if not isinstance(base_payload, list) or not isinstance(current_payload, list):
        return None
    base_rows = [dict(row) for row in base_payload if isinstance(row, dict)]
    current_rows = [dict(row) for row in current_payload if isinstance(row, dict)]
    if len(base_rows) != len(base_payload) or len(current_rows) != len(current_payload):
        return None
    base_by_id = _registry_rows_by_id(base_rows)
    current_by_id = _registry_rows_by_id(current_rows)
    if base_by_id is None or current_by_id is None:
        return None
    row_ids = [str(row["id"]) for row in current_rows]
    return {
        "schemaVersion": _JSON_JOURNAL_DELTA_SCHEMA_VERSION,
        "kind": "array_delta",
        "baseContentHash": _json_journal_payload_hash(base_rows),
        "contentHash": _json_journal_payload_hash(current_rows),
        "changed": [dict(row) for row in current_rows if base_by_id.get(str(row["id"])) != row],
        "removed": [row_id for row_id in base_by_id if row_id not in current_by_id],
        "rowIds": row_ids,
        "rowCount": len(current_rows),
        "timestamp": _json_journal_timestamp(),
    }


def _json_journal_object_delta_record(
    base_payload: Any,
    current_payload: Any,
) -> dict[str, Any] | None:
    if not isinstance(base_payload, dict) or not isinstance(current_payload, dict):
        return None
    if not all(isinstance(key, str) for key in base_payload) or not all(
        isinstance(key, str) for key in current_payload
    ):
        return None
    base_object = dict(base_payload)
    current_object = dict(current_payload)
    return {
        "schemaVersion": _JSON_JOURNAL_DELTA_SCHEMA_VERSION,
        "kind": "object_delta",
        "baseContentHash": _json_journal_payload_hash(base_object),
        "contentHash": _json_journal_payload_hash(current_object),
        "changed": {
            key: value for key, value in current_object.items() if base_object.get(key) != value
        },
        "removed": [key for key in base_object if key not in current_object],
        "rowCount": len(current_object),
        "timestamp": _json_journal_timestamp(),
    }


def _registry_journal_record(path: Path, payload: Any) -> dict[str, Any]:
    image_payload = _json_journal_image_payload(payload)
    base_payload = _registry_journal_base_payload(path)
    if isinstance(image_payload, list):
        delta_record = _json_journal_array_delta_record(base_payload, image_payload)
    elif isinstance(image_payload, dict):
        delta_record = _json_journal_object_delta_record(base_payload, image_payload)
    else:
        delta_record = None
    return delta_record if delta_record is not None else _json_journal_record(image_payload)


def _registry_journal_record_text(path: Path, payload: Any) -> str:
    return (
        json.dumps(
            _registry_journal_record(path, payload),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n"
    )


def _write_text_atomic(
    path: Path,
    text: str,
    *,
    policy: str = _WRITE_POLICY_REQUIRED,
) -> bool:
    from src import source_registry_io as _srio

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_data_dir()
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{time.time_ns()}.tmp")
    try:
        write_started_at = time.perf_counter()
        tmp.write_text(text, encoding="utf-8")
        replaced = _srio._replace_path_with_retry(
            tmp,
            path,
            policy=policy,
            allow_target_unlink=False,
        )
        record_jsonl_write(
            path=path,
            operation="rewrite",
            bytes_written=len(text.encode("utf-8")),
            duration_ms=duration_ms(write_started_at),
            row_count=sum(1 for line in text.splitlines() if line.strip()),
            replaced=replaced,
            data_dir=_storage_metrics_data_dir_for(path),
        )
        return replaced
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _append_json_journal_record(path: Path, payload: Any) -> None:
    from src import source_registry_io as _srio

    if _is_runtime_evidence_file(path):
        raise ValueError(
            f"Runtime evidence files must not be journaled: {_storage_base_name(path)}"
        )
    if not _uses_json_journal(path):
        raise ValueError(
            f"JSON journaling is registry-only; unsupported artifact: {_storage_base_name(path)}"
        )
    journal_path = _json_journal_path_for(path)
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    ensure_data_dir()
    record_text = _registry_journal_record_text(path, payload)
    record_bytes = len(record_text.encode("utf-8"))
    try:
        journal_bytes = journal_path.stat().st_size if journal_path.exists() else 0
    except OSError:
        journal_bytes = 0
    if journal_bytes + record_bytes > _srio._JSON_JOURNAL_HARD_MAX_BYTES:
        _srio._write_text_atomic(
            journal_path,
            record_text,
            policy=_WRITE_POLICY_REQUIRED,
        )
        return

    def _append_record() -> None:
        with journal_path.open("a", encoding="utf-8") as handle:
            handle.write(record_text)

    append_started_at = time.perf_counter()
    _run_write_with_retries(_append_record, policy=_WRITE_POLICY_REQUIRED)
    record_jsonl_write(
        path=journal_path,
        operation="append",
        bytes_written=record_bytes,
        duration_ms=duration_ms(append_started_at),
        row_count=1,
        data_dir=_storage_metrics_data_dir_for(path),
    )


def _compact_json_journal_if_needed(path: Path, payload: Any) -> None:
    from src import source_registry_io as _srio

    if not _uses_json_journal(path):
        return
    journal_path = _json_journal_path_for(path)
    try:
        if (
            not journal_path.exists()
            or journal_path.stat().st_size <= _srio._JSON_JOURNAL_COMPACT_MAX_BYTES
        ):
            return
    except OSError:
        return
    _srio._write_text_atomic(
        journal_path,
        _registry_journal_record_text(path, payload),
        policy=_WRITE_POLICY_REQUIRED,
    )
