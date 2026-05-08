"""JSON IO and path defaults for source registry files."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

from src.baluffo_config import get_storage_defaults

_STORAGE_DEFAULTS = get_storage_defaults()
_DEFAULT_DATA_DIR = _STORAGE_DEFAULTS["data_dir"]
DATA_DIR = Path(os.getenv("BALUFFO_DATA_DIR") or _DEFAULT_DATA_DIR).expanduser().resolve()
DEFAULTS_DIR = DATA_DIR / "defaults"
ACTIVE_PATH = DATA_DIR / "source-registry-active.json"
PENDING_PATH = DATA_DIR / "source-registry-pending.json"
ACTIVE_SEED_PATH = DEFAULTS_DIR / "source-registry-active.seed.json"
PENDING_SEED_PATH = DEFAULTS_DIR / "source-registry-pending.seed.json"
REJECTED_PATH = DATA_DIR / "source-registry-rejected.json"
DISCOVERY_REPORT_PATH = DATA_DIR / "source-discovery-report.json"
DISCOVERY_CANDIDATES_PATH = DATA_DIR / "source-discovery-candidates.json"
M5_STRATEGIC_BACKLOG_PATH = DATA_DIR / "m5-strategic-backlog.json"
URL_PATCH_MANIFEST_PATH = DATA_DIR / "url-patch-manifest.json"
APPROVAL_STATE_PATH = DATA_DIR / "source-approval-state.json"
TOMBSTONES_PATH = DATA_DIR / "source-registry-tombstones.json"

_REGISTRY_SEED_NAMES = {
    "source-registry-active.json": "source-registry-active.seed.json",
    "source-registry-pending.json": "source-registry-pending.seed.json",
}

_LEAN_REGISTRY_ENTRYPOINT_NAMES = {
    "source-registry-active.json",
    "source-registry-pending.json",
}

_GZIP_REGISTRY_NAMES = {
    "source-registry-active.json",
    "source-registry-pending.json",
    "source-registry-rejected.json",
    "source-registry-tombstones.json",
    "source-registry-metadata.json",
}

_JSON_JOURNAL_SCHEMA_VERSION = 1
_JSON_JOURNAL_COMPACT_MAX_BYTES = 1_048_576
_WRITE_POLICY_REQUIRED = "required"
_WRITE_POLICY_BEST_EFFORT = "best_effort"
_WRITE_RETRY_ATTEMPTS = 18
_WRITE_RETRY_BACKOFF_BASE_S = 0.012


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _storage_base_name(path: Path) -> str:
    return Path(path).name.removesuffix(".gz")


def _uses_gzip_storage(path: Path) -> bool:
    return _storage_base_name(path) in _GZIP_REGISTRY_NAMES


def _gzip_path_for(path: Path) -> Path:
    return path if path.suffix == ".gz" else path.with_name(path.name + ".gz")


def _json_storage_candidates(path: Path) -> list[Path]:
    path = Path(path)
    if not _uses_gzip_storage(path):
        return [path]
    compressed = _gzip_path_for(path)
    if compressed == path:
        return [path, path.with_suffix("")]
    return [compressed, path]


def registry_seed_path_for(path: Path) -> Path | None:
    seed_name = _REGISTRY_SEED_NAMES.get(_storage_base_name(Path(path)))
    if seed_name is None:
        return None
    return Path(path).parent / "defaults" / seed_name


_LEAN_REGISTRY_CORE_FIELDS = (
    "id",
    "name",
    "adapter",
    "studio",
    "registryState",
    "pendingReason",
    "stateChangedAt",
    "stateChangedBy",
    "lastPromotedAt",
    "lastDemotedAt",
)


def _is_lean_registry_entrypoint(path: Path) -> bool:
    return _storage_base_name(Path(path)) in _LEAN_REGISTRY_ENTRYPOINT_NAMES


def _registry_metadata_path_for(path: Path) -> Path | None:
    if not _is_lean_registry_entrypoint(path):
        return None
    return Path(path).with_name("source-registry-metadata.json.gz")


def _registry_counterpart_path(path: Path) -> Path | None:
    base_name = _storage_base_name(Path(path))
    if base_name == "source-registry-active.json":
        return Path(path).with_name("source-registry-pending.json")
    if base_name == "source-registry-pending.json":
        return Path(path).with_name("source-registry-active.json")
    return None


def _load_runtime_json_array(
    path: Path, default: list[dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    return load_json_array(path, default)


def _lean_registry_core_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: row[key] for key in _LEAN_REGISTRY_CORE_FIELDS if key in row}


def _lean_registry_sparse_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key not in _LEAN_REGISTRY_CORE_FIELDS}


def _lean_registry_metadata_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    for row in rows:
        source_id = str(row.get("id") or "").strip()
        if not source_id:
            continue
        sparse = _lean_registry_sparse_row(row)
        if sparse:
            metadata[source_id] = sparse
    return metadata


def _merge_lean_registry_rows(
    rows: list[dict[str, Any]], metadata_map: dict[str, Any]
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for row in rows:
        merged_row = dict(row)
        source_id = str(merged_row.get("id") or "").strip()
        metadata = metadata_map.get(source_id)
        if source_id and isinstance(metadata, dict):
            for key, value in metadata.items():
                if key not in _LEAN_REGISTRY_CORE_FIELDS:
                    merged_row[key] = value
        merged.append(merged_row)
    return merged


def _prepare_lean_registry_rows_for_write(
    path: Path, payload: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    full_merged_rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for row in payload:
        row_copy = dict(row)
        source_id = str(row_copy.get("id") or "").strip()
        if not source_id:
            raise ValueError("registry rows must include a source id")
        if source_id in seen_ids:
            raise ValueError(f"duplicate registry source id in payload: {source_id}")
        seen_ids.add(source_id)
        full_merged_rows.append(row_copy)
    counterpart_path = _registry_counterpart_path(path)
    counterpart_rows = _load_runtime_json_array(counterpart_path, []) if counterpart_path else []
    combined_rows = full_merged_rows + counterpart_rows
    combined_by_id: dict[str, dict[str, Any]] = {}
    for row in combined_rows:
        source_id = str(row.get("id") or "").strip()
        if not source_id:
            raise ValueError("registry rows must include a source id")
        if source_id in combined_by_id:
            raise ValueError(f"duplicate registry source id across active and pending: {source_id}")
        combined_by_id[source_id] = dict(row)
    return (
        [_lean_registry_core_row(row) for row in full_merged_rows],
        _lean_registry_metadata_map(list(combined_by_id.values())),
    )


def _prepare_lean_registry_rows_for_batch_write(
    active_payload: list[dict[str, Any]], pending_payload: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    combined_by_id: dict[str, dict[str, Any]] = {}
    for scope, payload in (("active", active_payload), ("pending", pending_payload)):
        seen_ids: set[str] = set()
        for row in payload:
            row_copy = dict(row)
            source_id = str(row_copy.get("id") or "").strip()
            if not source_id:
                raise ValueError("registry rows must include a source id")
            if source_id in seen_ids:
                raise ValueError(f"duplicate registry source id in {scope} payload: {source_id}")
            if source_id in combined_by_id:
                raise ValueError(
                    f"duplicate registry source id across active and pending: {source_id}"
                )
            seen_ids.add(source_id)
            combined_by_id[source_id] = row_copy
    return (
        [_lean_registry_core_row(row) for row in active_payload],
        [_lean_registry_core_row(row) for row in pending_payload],
        _lean_registry_metadata_map(list(combined_by_id.values())),
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


def _load_json_payload_from_file(path: Path) -> Any | None:
    try:
        if path.suffix == ".gz":
            with gzip.open(path, mode="rt", encoding="utf-8") as handle:
                return json.load(handle)
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def load_json_array(
    path: Path, default: list[dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    fallback = default or []
    path = Path(path)
    rows = _load_json_array_from_storage(path, fallback)
    if rows is None:
        rows = [dict(row) for row in fallback]
    journal_rows = _load_json_journal_latest_payload(path)
    if isinstance(journal_rows, list):
        return [dict(row) for row in journal_rows if isinstance(row, dict)]
    return rows


def load_json_object(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback = dict(default or {})
    try:
        source_path = Path(path)
        candidates = _json_storage_candidates(source_path)
        existing = next((candidate for candidate in candidates if candidate.exists()), None)
        if existing is None:
            base_payload = fallback
        elif existing.suffix == ".gz":
            with gzip.open(existing, mode="rt", encoding="utf-8") as handle:
                payload = json.load(handle)
            base_payload = payload if isinstance(payload, dict) else fallback
        else:
            payload = json.loads(existing.read_text(encoding="utf-8"))
            base_payload = payload if isinstance(payload, dict) else fallback
        if _json_journal_should_overlay_base(source_path, existing):
            journal_payload = _load_json_journal_latest_payload(source_path)
            if isinstance(journal_payload, dict):
                return dict(journal_payload)
        return dict(base_payload)
    except (OSError, json.JSONDecodeError):
        journal_payload = _load_json_journal_latest_payload(Path(path))
        if isinstance(journal_payload, dict):
            return dict(journal_payload)
        return fallback


def _sleep_after_write_failure(attempt: int) -> None:
    time.sleep(_WRITE_RETRY_BACKOFF_BASE_S * (attempt + 1))


def _finish_write_failure(error: OSError, policy: str) -> bool:
    if policy == _WRITE_POLICY_BEST_EFFORT:
        return False
    raise error


def _run_write_with_retries(operation, *, policy: str = _WRITE_POLICY_REQUIRED) -> bool:
    last_error: OSError | None = None
    for attempt in range(_WRITE_RETRY_ATTEMPTS):
        try:
            operation()
            return True
        except OSError as exc:
            last_error = exc
            _sleep_after_write_failure(attempt)
    if last_error is None:
        return True
    return _finish_write_failure(last_error, policy)


def _replace_path_with_retry(
    tmp: Path,
    target: Path,
    *,
    policy: str = _WRITE_POLICY_REQUIRED,
) -> bool:
    last_error: OSError | None = None
    for attempt in range(_WRITE_RETRY_ATTEMPTS):
        try:
            os.replace(tmp, target)
            return True
        except OSError as exc:
            last_error = exc
            _sleep_after_write_failure(attempt)
    if last_error is not None and policy == _WRITE_POLICY_REQUIRED and target.exists():
        try:
            target.unlink()
            os.replace(tmp, target)
            return True
        except OSError as exc:
            last_error = exc
    if last_error is None:
        return True
    return _finish_write_failure(last_error, policy)


def _write_json_payload_atomic(
    path: Path,
    payload: Any,
    *,
    policy: str = _WRITE_POLICY_REQUIRED,
) -> bool:
    path = Path(path)
    target = _gzip_path_for(path) if _uses_gzip_storage(path) else path
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_data_dir()
    # Use a unique temp file per write to avoid collisions across threads/processes.
    tmp = target.with_suffix(target.suffix + f".{os.getpid()}.{time.time_ns()}.tmp")
    try:
        serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        if target.suffix == ".gz":
            with gzip.open(tmp, mode="wt", encoding="utf-8") as handle:
                handle.write(serialized)
        else:
            tmp.write_text(serialized, encoding="utf-8")
        return _replace_path_with_retry(tmp, target, policy=policy)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _json_journal_path_for(path: Path) -> Path:
    base_name = _storage_base_name(Path(path))
    if base_name.endswith(".json"):
        return Path(path).with_name(f"{base_name[:-5]}.jsonl")
    return Path(path).with_name(f"{base_name}.jsonl")


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


def _write_text_atomic(
    path: Path,
    text: str,
    *,
    policy: str = _WRITE_POLICY_REQUIRED,
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_data_dir()
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{time.time_ns()}.tmp")
    try:
        tmp.write_text(text, encoding="utf-8")
        _replace_path_with_retry(tmp, path, policy=policy)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _append_json_journal_record(path: Path, payload: Any) -> None:
    journal_path = _json_journal_path_for(path)
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    ensure_data_dir()
    record_text = _json_journal_record_text(payload)

    def _append_record() -> None:
        with journal_path.open("a", encoding="utf-8") as handle:
            handle.write(record_text)

    _run_write_with_retries(_append_record, policy=_WRITE_POLICY_REQUIRED)


def _compact_json_journal_if_needed(path: Path, payload: Any) -> None:
    journal_path = _json_journal_path_for(path)
    try:
        if (
            not journal_path.exists()
            or journal_path.stat().st_size <= _JSON_JOURNAL_COMPACT_MAX_BYTES
        ):
            return
    except OSError:
        return
    _write_text_atomic(
        journal_path,
        _json_journal_record_text(payload),
        policy=_WRITE_POLICY_BEST_EFFORT,
    )


def _load_json_journal_record_payload(record: Any) -> Any | None:
    if not isinstance(record, dict):
        return None
    try:
        schema_version = int(record.get("schemaVersion") or 0)
    except (TypeError, ValueError):
        return None
    if schema_version != _JSON_JOURNAL_SCHEMA_VERSION:
        return None
    payload = record.get("payload")
    try:
        content_hash = _json_journal_payload_hash(payload)
    except (TypeError, ValueError):
        return None
    if str(record.get("contentHash") or "") != content_hash:
        return None
    return payload


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


def _load_json_journal_latest_payload(path: Path) -> Any | None:
    journal_path = _json_journal_path_for(path)
    if not journal_path.exists():
        return None
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
                payload = _load_json_journal_record_payload(record)
                if payload is not None:
                    latest_payload = payload
    except OSError:
        return None
    return latest_payload


def _json_payload_matches_existing(path: Path, payload: Any) -> bool:
    if isinstance(payload, list):
        if not any(candidate.exists() for candidate in _json_storage_candidates(path)) and not (
            _json_journal_path_for(path).exists()
        ):
            return False
        return load_json_array(path, []) == _json_journal_image_payload(payload)
    if isinstance(payload, dict):
        if not any(candidate.exists() for candidate in _json_storage_candidates(path)) and not (
            _json_journal_path_for(path).exists()
        ):
            return False
        return load_json_object(path, {}) == dict(payload)
    target = _gzip_path_for(path) if _uses_gzip_storage(path) else path
    if not target.exists():
        return False
    return _load_json_payload_from_file(target) == payload


def save_json_atomic(path: Path, payload: Any) -> None:
    path = Path(path)
    if _json_payload_matches_existing(path, payload):
        return
    journal_payload = _json_journal_image_payload(payload)
    if (
        _is_lean_registry_entrypoint(path)
        and isinstance(journal_payload, list)
        and all(isinstance(row, dict) for row in journal_payload)
    ):
        core_rows, metadata_map = _prepare_lean_registry_rows_for_write(path, journal_payload)
        _append_json_journal_record(path, journal_payload)
        _write_json_payload_atomic(path, core_rows)
        metadata_path = _registry_metadata_path_for(path)
        if metadata_path is not None:
            _write_json_payload_atomic(
                metadata_path,
                metadata_map,
                policy=_WRITE_POLICY_BEST_EFFORT,
            )
        _compact_json_journal_if_needed(path, journal_payload)
        return
    _append_json_journal_record(path, journal_payload)
    _write_json_payload_atomic(path, journal_payload)
    _compact_json_journal_if_needed(path, journal_payload)


def save_registry_state_atomic(
    active_path: Path,
    pending_path: Path,
    rejected_path: Path,
    state: dict[str, list[dict[str, Any]]],
) -> None:
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
    _append_json_journal_record(active_path, active_payload)
    _append_json_journal_record(pending_path, pending_payload)
    _write_json_payload_atomic(active_path, active_core)
    _write_json_payload_atomic(pending_path, pending_core)
    metadata_path = _registry_metadata_path_for(active_path)
    if metadata_path is not None:
        _write_json_payload_atomic(
            metadata_path,
            metadata_map,
            policy=_WRITE_POLICY_BEST_EFFORT,
        )
    _compact_json_journal_if_needed(active_path, active_payload)
    _compact_json_journal_if_needed(pending_path, pending_payload)
    save_json_atomic(rejected_path, rejected_payload)
