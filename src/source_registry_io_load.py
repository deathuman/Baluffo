"""Source registry loaders and storage summaries.

AI boundary owns: runtime-evidence / array / object loading with journal and gzip dispatch,
storage summaries, the raw file readers shared with the journal leaf, the array-read
chain (_load_json_array_rows_from_path / _load_json_array_from_storage /
_load_json_journal_latest_payload) that the public loaders dispatch through, and journal
delta-payload parsing (record payloads / array-object deltas).
AI boundary implement in: this leaf for loaders; path classification comes from
``source_registry_io_paths.py`` and journal records from ``source_registry_io_journal.py``.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

from src.source_registry_io_journal import (
    _json_journal_path_for,
    _json_journal_payload_hash,
    _json_journal_should_overlay_base,
    _registry_journal_base_payload,
    _registry_rows_by_id,
)
from src.source_registry_io_paths import (
    _JSON_JOURNAL_DELTA_SCHEMA_VERSION,
    _JSON_JOURNAL_SCHEMA_VERSION,
    _is_runtime_evidence_file,
    _json_storage_candidates,
    _merge_lean_registry_rows,
    _registry_metadata_path_for,
    _storage_base_name,
    _uses_json_journal,
    registry_seed_path_for,
)


def _load_json_array_from_file(path: Path, fallback: list[dict[str, Any]]) -> list[dict[str, Any]]:
    try:
        if path.suffix == ".gz":
            with gzip.open(path, mode="rt", encoding="utf-8") as handle:
                payload = json.load(handle)
        else:
            payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return [dict(row) for row in fallback]
        return [row for row in payload if isinstance(row, dict)]
    except (OSError, json.JSONDecodeError):
        return [dict(row) for row in fallback]


def _load_json_object_from_storage(
    path: Path,
    fallback: dict[str, Any],
) -> dict[str, Any]:
    try:
        source_path = Path(path)
        existing = next(
            (
                candidate
                for candidate in _json_storage_candidates(source_path)
                if candidate.exists()
            ),
            None,
        )
        if existing is None:
            return dict(fallback)
        if existing.suffix == ".gz":
            with gzip.open(existing, mode="rt", encoding="utf-8") as handle:
                payload = json.load(handle)
        else:
            payload = json.loads(existing.read_text(encoding="utf-8"))
        return dict(payload) if isinstance(payload, dict) else dict(fallback)
    except (OSError, json.JSONDecodeError):
        return dict(fallback)


def _load_json_payload_from_file(path: Path) -> Any | None:
    try:
        if path.suffix == ".gz":
            with gzip.open(path, mode="rt", encoding="utf-8") as handle:
                return json.load(handle)
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def load_runtime_evidence(path: Any, default: dict[str, Any] | None = None) -> dict[str, Any]:
    """Read canonical runtime evidence JSON directly with no journal overlay.

    Runtime evidence files (fetch reports, fetch tasks, discovery reports,
    sync live task files, etc.) must never be shadowed by .jsonl journals.
    This function reads only the canonical file; it does not check for
    adjacent journals, apply mtime comparison, or fall through to journal data.

    Args:
        path: Path to the canonical JSON file.
        default: Fallback dict returned when the file is absent or corrupt.

    Returns:
        A dict copy of the parsed payload, or a copy of *default* on failure.
    """
    fallback = dict(default or {})
    try:
        source_path = Path(path)
        candidates = _json_storage_candidates(source_path)
        existing = next((candidate for candidate in candidates if candidate.exists()), None)
        if existing is None:
            return fallback
        payload = _load_json_payload_from_file(existing)
        if isinstance(payload, dict):
            return dict(payload)
        return fallback
    except (OSError, json.JSONDecodeError):
        return fallback


def load_runtime_evidence_array(
    path: Any, default: list[dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    """Read canonical runtime evidence array JSON directly with no journal overlay."""

    fallback = [dict(row) for row in (default or []) if isinstance(row, dict)]
    try:
        source_path = Path(path)
        candidates = _json_storage_candidates(source_path)
        existing = next((candidate for candidate in candidates if candidate.exists()), None)
        if existing is None:
            return fallback
        payload = _load_json_payload_from_file(existing)
        if isinstance(payload, list):
            return [dict(row) for row in payload if isinstance(row, dict)]
        return fallback
    except (OSError, json.JSONDecodeError):
        return fallback


def load_json_array(
    path: Path, default: list[dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    fallback = default or []
    path = Path(path)
    base_name = _storage_base_name(path)
    if _is_runtime_evidence_file(path):
        raise RuntimeError(
            f"load_json_array must not be used for runtime evidence files. "
            f'Use load_runtime_evidence or load_runtime_evidence_array for "{base_name}" instead.'
        )
    rows = _load_json_array_from_storage(path, fallback)
    if rows is None:
        rows = [dict(row) for row in fallback]
    # Determine the canonical file that was loaded so we can compare mtimes.
    existing = next(
        (candidate for candidate in _json_storage_candidates(path) if candidate.exists()),
        None,
    )
    if _uses_json_journal(path) and _json_journal_should_overlay_base(path, existing):
        journal_rows = _load_json_journal_latest_payload(path, base_payload=rows)
        if isinstance(journal_rows, list):
            return [dict(row) for row in journal_rows if isinstance(row, dict)]
    return rows


def _summary_text(value: Any) -> str:
    return str(value or "").strip()


def _summary_lower(value: Any) -> str:
    return _summary_text(value).lower()


def _summary_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _summary_pending_is_hidden(row: dict[str, Any]) -> bool:
    return (
        bool(row.get("hiddenFromDefault")) or _summary_lower(row.get("candidateState")) == "hidden"
    )


def _summary_pending_is_deferred(row: dict[str, Any]) -> bool:
    return (
        bool(row.get("deferred"))
        or bool(_summary_text(row.get("deferReason")))
        or bool(_summary_text(row.get("firstDeferredAt")))
        or bool(_summary_text(row.get("lastDeferredAt")))
        or _summary_int(row.get("deferCount")) > 0
    )


def _summary_pending_is_duplicate(row: dict[str, Any]) -> bool:
    return bool(_summary_text(row.get("duplicateOfSourceId"))) or "duplicate" in _summary_lower(
        row.get("pendingReason")
    )


def summarize_json_array_storage(
    path: Path, default: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    """Return cheap storage metadata for a registry JSON array.

    This intentionally avoids lean-row metadata expansion and registry
    normalization. It is for startup summaries where count/fingerprint evidence
    is enough and the full registry detail route can do exact derivation later.
    """

    fallback = [dict(row) for row in (default or []) if isinstance(row, dict)]
    source_path = Path(path)
    existing = next(
        (candidate for candidate in _json_storage_candidates(source_path) if candidate.exists()),
        None,
    )
    payload: Any = fallback
    status = "fallback"
    if existing is not None:
        try:
            payload = _load_json_payload_from_file(existing)
            status = "ready" if isinstance(payload, list) else "invalid"
        except (OSError, json.JSONDecodeError):
            payload = fallback
            status = "unreadable"
    if not isinstance(payload, list):
        payload = fallback
    if _uses_json_journal(source_path) and _json_journal_should_overlay_base(source_path, existing):
        journal_payload = _load_json_journal_latest_payload(source_path, base_payload=payload)
        if isinstance(journal_payload, list):
            payload = journal_payload
            status = "journal"
    journal_path = _json_journal_path_for(source_path)
    signatures = []
    for candidate in (existing, journal_path if journal_path.exists() else None):
        if candidate is None:
            continue
        try:
            stat = candidate.stat()
        except OSError:
            continue
        signatures.append(
            {
                "path": candidate.name,
                "size": int(stat.st_size),
                "mtimeNs": int(stat.st_mtime_ns),
            }
        )
    rows = [row for row in payload if isinstance(row, dict)]
    return {
        "count": len(rows),
        "invalidCount": max(0, len(payload) - len(rows)),
        "hiddenCount": sum(1 for row in rows if _summary_pending_is_hidden(row)),
        "deferredCount": sum(1 for row in rows if _summary_pending_is_deferred(row)),
        "duplicateCount": sum(1 for row in rows if _summary_pending_is_duplicate(row)),
        "status": status,
        "storage": signatures,
    }


def load_json_object(path: Path, default: Any = None) -> dict[str, Any]:
    fallback = dict(default or {})
    try:
        source_path = Path(path)
        existing = next(
            (
                candidate
                for candidate in _json_storage_candidates(source_path)
                if candidate.exists()
            ),
            None,
        )
        base_payload = _load_json_object_from_storage(source_path, fallback)
        if _uses_json_journal(source_path) and _json_journal_should_overlay_base(
            source_path, existing
        ):
            journal_payload = _load_json_journal_latest_payload(
                source_path,
                base_payload=base_payload,
            )
            if isinstance(journal_payload, dict):
                return dict(journal_payload)
        return dict(base_payload)
    except (OSError, json.JSONDecodeError):
        source_path = Path(path)
        if _uses_json_journal(source_path):
            journal_payload = _load_json_journal_latest_payload(
                source_path,
                base_payload=fallback,
            )
            if isinstance(journal_payload, dict):
                return dict(journal_payload)
        return fallback


def _load_json_journal_array_delta_payload(
    record: dict[str, Any],
    base_payload: Any,
) -> list[dict[str, Any]] | None:
    base_rows = _json_journal_dict_rows(base_payload)
    if base_rows is None:
        return None
    if str(record.get("baseContentHash") or "") != _json_journal_payload_hash(base_rows):
        return None
    rows_by_id = _registry_rows_by_id(base_rows)
    if rows_by_id is None:
        return None
    removed = _json_journal_string_list(record.get("removed"))
    changed = _json_journal_dict_rows(record.get("changed"))
    row_ids = _json_journal_string_list(record.get("rowIds"))
    if removed is None or changed is None or row_ids is None:
        return None
    for row_id in removed:
        rows_by_id.pop(row_id, None)
    for row in changed:
        candidate_row_id = row.get("id")
        if not isinstance(candidate_row_id, str) or not candidate_row_id:
            return None
        rows_by_id[candidate_row_id] = dict(row)
    if set(rows_by_id) != set(row_ids):
        return None
    payload = [rows_by_id[row_id] for row_id in row_ids]
    return payload if _json_journal_array_delta_matches_record(record, payload) else None


def _json_journal_dict_rows(value: Any) -> list[dict[str, Any]] | None:
    if not isinstance(value, list):
        return None
    rows = [dict(row) for row in value if isinstance(row, dict)]
    return rows if len(rows) == len(value) else None


def _json_journal_string_list(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None
    rows = [row for row in value if isinstance(row, str)]
    return rows if len(rows) == len(value) else None


def _json_journal_array_delta_matches_record(
    record: dict[str, Any], payload: list[dict[str, Any]]
) -> bool:
    if _json_journal_record_row_count(record) != len(payload):
        return False
    return str(record.get("contentHash") or "") == _json_journal_payload_hash(payload)


def _load_json_journal_object_delta_payload(
    record: dict[str, Any],
    base_payload: Any,
) -> dict[str, Any] | None:
    if not isinstance(base_payload, dict):
        return None
    if str(record.get("baseContentHash") or "") != _json_journal_payload_hash(base_payload):
        return None
    removed = record.get("removed")
    changed = record.get("changed")
    if not isinstance(removed, list) or not all(isinstance(key, str) for key in removed):
        return None
    if not isinstance(changed, dict):
        return None
    payload = dict(base_payload)
    for key in removed:
        payload.pop(key, None)
    payload.update(changed)
    if _json_journal_record_row_count(record) != len(payload):
        return None
    if str(record.get("contentHash") or "") != _json_journal_payload_hash(payload):
        return None
    return payload


def _json_journal_record_row_count(record: dict[str, Any]) -> int | None:
    row_count = record.get("rowCount")
    if row_count is None:
        return None
    try:
        return int(row_count)
    except (TypeError, ValueError):
        return None


def _load_json_journal_record_payload(
    record: Any,
    base_payload: Any | None = None,
) -> Any | None:
    if not isinstance(record, dict):
        return None
    try:
        schema_version = int(record.get("schemaVersion") or 0)
    except (TypeError, ValueError):
        return None
    if schema_version == _JSON_JOURNAL_SCHEMA_VERSION:
        payload = record.get("payload")
        try:
            content_hash = _json_journal_payload_hash(payload)
        except (TypeError, ValueError):
            return None
        if str(record.get("contentHash") or "") != content_hash:
            return None
        return payload
    if schema_version != _JSON_JOURNAL_DELTA_SCHEMA_VERSION:
        return None
    if record.get("kind") == "array_delta":
        return _load_json_journal_array_delta_payload(record, base_payload or [])
    if record.get("kind") == "object_delta":
        return _load_json_journal_object_delta_payload(record, base_payload or {})
    return None


def _load_json_array_rows_from_path(
    registry_path: Path, source_path: Path, fallback: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    rows = _load_json_array_from_file(source_path, fallback)
    metadata_path = _registry_metadata_path_for(registry_path)
    if metadata_path is None:
        return rows
    metadata_payload = load_json_object(metadata_path, {})
    if isinstance(metadata_payload, dict) and metadata_payload:
        return _merge_lean_registry_rows(rows, metadata_payload)
    return rows


def _load_json_array_from_storage(
    path: Path, fallback: list[dict[str, Any]]
) -> list[dict[str, Any]] | None:
    for candidate in _json_storage_candidates(path):
        if candidate.exists():
            return _load_json_array_rows_from_path(path, candidate, fallback)
    seed_path = registry_seed_path_for(path)
    if seed_path is not None and seed_path.exists():
        return _load_json_array_rows_from_path(path, seed_path, fallback)
    return None


def _load_json_journal_latest_payload(
    path: Path,
    *,
    base_payload: Any | None = None,
) -> Any | None:
    journal_path = _json_journal_path_for(path)
    if not journal_path.exists():
        return None
    current_base = _registry_journal_base_payload(path) if base_payload is None else base_payload
    latest_payload: Any | None = None
    try:
        with journal_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                if not raw_line.strip():
                    continue
                try:
                    record = json.loads(raw_line)
                except json.JSONDecodeError:
                    break
                payload = _load_json_journal_record_payload(
                    record,
                    base_payload=current_base,
                )
                if payload is not None:
                    latest_payload = payload
                    current_base = payload
    except OSError:
        return None
    return latest_payload
