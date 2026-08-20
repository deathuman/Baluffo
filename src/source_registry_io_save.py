"""Source registry atomic save entrypoints.

AI boundary owns: payload/existing comparison, atomic JSON payload writes, and the public
``save_json_atomic`` / ``save_registry_state_atomic`` entrypoints.
AI boundary implement in: this leaf for saves; retry/atomic primitives come from
``source_registry_io_paths.py``, journal appends from ``source_registry_io_journal.py``, and
loaders from ``source_registry_io_load.py``.
"""

from __future__ import annotations

import gzip
import json
import os
import time
from pathlib import Path
from typing import Any

from src.source_registry_io_journal import (
    _compact_json_journal_if_needed,
    _json_journal_image_payload,
    _json_journal_path_for,
)
from src.source_registry_io_load import (
    _load_json_payload_from_file,
    load_json_array,
    load_json_object,
)
from src.source_registry_io_paths import (
    _WRITE_POLICY_BEST_EFFORT,
    _WRITE_POLICY_REQUIRED,
    _gzip_path_for,
    _is_lean_registry_entrypoint,
    _is_runtime_evidence_file,
    _json_storage_candidates,
    _prepare_lean_registry_rows_for_batch_write,
    _prepare_lean_registry_rows_for_write,
    _registry_metadata_path_for,
    _storage_metrics_data_dir_for,
    _uses_gzip_storage,
    _uses_json_journal,
    ensure_data_dir,
)
from src.storage_metrics import duration_ms, record_json_write


def _write_json_payload_atomic(
    path: Path,
    payload: Any,
    *,
    policy: str = _WRITE_POLICY_REQUIRED,
) -> bool:
    from src import source_registry_io as _srio

    path = Path(path)
    target = _gzip_path_for(path) if _uses_gzip_storage(path) else path
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_data_dir()
    # Use a unique temp file per write to avoid collisions across threads/processes.
    tmp = target.with_suffix(target.suffix + f".{os.getpid()}.{time.time_ns()}.tmp")
    try:
        serialization_started_at = time.perf_counter()
        serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        serialization_duration_ms = duration_ms(serialization_started_at)
        uncompressed_size_bytes = len(serialized.encode("utf-8"))
        if target.suffix == ".gz":
            with gzip.open(tmp, mode="wt", encoding="utf-8") as handle:
                handle.write(serialized)
        else:
            tmp.write_text(serialized, encoding="utf-8")
        try:
            compressed_size_bytes = tmp.stat().st_size
        except OSError:
            compressed_size_bytes = uncompressed_size_bytes
        replace_started_at = time.perf_counter()
        replaced = _srio._replace_path_with_retry(tmp, target, policy=policy)
        record_json_write(
            path=path,
            target=target,
            storage_kind="gzip" if target.suffix == ".gz" else "json",
            serialization_duration_ms=serialization_duration_ms,
            atomic_replace_duration_ms=duration_ms(replace_started_at),
            compressed_size_bytes=compressed_size_bytes,
            uncompressed_size_bytes=uncompressed_size_bytes,
            replaced=replaced,
            data_dir=_storage_metrics_data_dir_for(path),
        )
        if replaced and target.suffix == ".gz" and path.suffix != ".gz":
            _remove_stale_plain_json_storage_file(path)
        return replaced
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _remove_stale_plain_json_storage_file(path: Path) -> None:
    """Remove the legacy plain JSON copy after gzip-backed storage is written."""
    if not _uses_gzip_storage(path):
        return
    plain_path = Path(path)
    if plain_path.suffix == ".gz":
        plain_path = plain_path.with_suffix("")
    compressed_path = _gzip_path_for(plain_path)
    if plain_path == compressed_path or not plain_path.exists():
        return
    try:
        plain_path.unlink()
    except OSError:
        pass


def _json_payload_matches_existing(path: Path, payload: Any) -> bool:
    if not _uses_json_journal(path):
        return _canonical_json_payload_matches_existing(path, payload)
    if isinstance(payload, list):
        if not any(candidate.exists() for candidate in _json_storage_candidates(path)) and not (
            _json_journal_path_for(path).exists()
        ):
            return False
        return bool(load_json_array(path, []) == _json_journal_image_payload(payload))
    if isinstance(payload, dict):
        if not any(candidate.exists() for candidate in _json_storage_candidates(path)) and not (
            _json_journal_path_for(path).exists()
        ):
            return False
        return bool(load_json_object(path, {}) == dict(payload))
    target = _gzip_path_for(path) if _uses_gzip_storage(path) else path
    if not target.exists():
        return False
    return bool(_load_json_payload_from_file(target) == payload)


def _canonical_json_payload_matches_existing(path: Path, payload: Any) -> bool:
    existing = next(
        (candidate for candidate in _json_storage_candidates(path) if candidate.exists()),
        None,
    )
    if existing is None:
        return False
    try:
        return bool(_load_json_payload_from_file(existing) == _json_journal_image_payload(payload))
    except (OSError, json.JSONDecodeError):
        return False


def save_json_atomic(path: Path, payload: Any) -> None:
    from src import source_registry_io as _srio

    path = Path(path)
    if _is_runtime_evidence_file(path):
        if _canonical_json_payload_matches_existing(path, payload):
            return
        _srio._write_json_payload_atomic(path, _json_journal_image_payload(payload))
        return
    if not _uses_json_journal(path):
        if _canonical_json_payload_matches_existing(path, payload):
            return
        _srio._write_json_payload_atomic(path, _json_journal_image_payload(payload))
        return
    if _json_payload_matches_existing(path, payload):
        if _uses_gzip_storage(path) and _gzip_path_for(path).exists():
            _remove_stale_plain_json_storage_file(path)
        return
    journal_payload = _json_journal_image_payload(payload)
    if (
        _is_lean_registry_entrypoint(path)
        and isinstance(journal_payload, list)
        and all(isinstance(row, dict) for row in journal_payload)
    ):
        core_rows, metadata_map = _prepare_lean_registry_rows_for_write(path, journal_payload)
        _srio._append_json_journal_record(path, journal_payload)
        _srio._write_json_payload_atomic(path, core_rows)
        metadata_path = _registry_metadata_path_for(path)
        if metadata_path is not None:
            _srio._write_json_payload_atomic(
                metadata_path,
                metadata_map,
                policy=_WRITE_POLICY_BEST_EFFORT,
            )
        _compact_json_journal_if_needed(path, journal_payload)
        return
    _srio._append_json_journal_record(path, journal_payload)
    _srio._write_json_payload_atomic(path, journal_payload)
    _compact_json_journal_if_needed(path, journal_payload)


def save_registry_state_atomic(
    active_path: Path,
    pending_path: Path,
    rejected_path: Path,
    state: dict[str, list[dict[str, Any]]],
) -> None:
    from src import source_registry_io as _srio

    active_payload = [dict(row) for row in list(state.get("active") or []) if isinstance(row, dict)]
    pending_payload = [
        dict(row) for row in list(state.get("pending") or []) if isinstance(row, dict)
    ]
    rejected_payload = [
        dict(row) for row in list(state.get("rejected") or []) if isinstance(row, dict)
    ]
    if not (
        _is_lean_registry_entrypoint(active_path) and _is_lean_registry_entrypoint(pending_path)
    ):
        save_json_atomic(active_path, active_payload)
        save_json_atomic(pending_path, pending_payload)
        save_json_atomic(rejected_path, rejected_payload)
        return
    active_core, pending_core, metadata_map = _prepare_lean_registry_rows_for_batch_write(
        active_payload,
        pending_payload,
    )
    _srio._append_json_journal_record(active_path, active_payload)
    _srio._append_json_journal_record(pending_path, pending_payload)
    _srio._write_json_payload_atomic(active_path, active_core)
    _srio._write_json_payload_atomic(pending_path, pending_core)
    metadata_path = _registry_metadata_path_for(active_path)
    if metadata_path is not None:
        _srio._write_json_payload_atomic(
            metadata_path,
            metadata_map,
            policy=_WRITE_POLICY_BEST_EFFORT,
        )
    _compact_json_journal_if_needed(active_path, active_payload)
    _compact_json_journal_if_needed(pending_path, pending_payload)
    save_json_atomic(rejected_path, rejected_payload)
