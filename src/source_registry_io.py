"""JSON IO and path defaults for source registry files."""

from __future__ import annotations

import gzip
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
    fallback = default or []
    path = Path(path)
    for candidate in _json_storage_candidates(path):
        if candidate.exists():
            rows = _load_json_array_from_file(candidate, fallback)
            metadata_path = _registry_metadata_path_for(path)
            if metadata_path is not None:
                metadata_payload = load_json_object(metadata_path, {})
                if isinstance(metadata_payload, dict) and metadata_payload:
                    rows = _merge_lean_registry_rows(rows, metadata_payload)
            return rows
    return [dict(row) for row in fallback]


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
    for candidate in _json_storage_candidates(path):
        if candidate.exists():
            rows = _load_json_array_from_file(candidate, fallback)
            metadata_path = _registry_metadata_path_for(path)
            if metadata_path is not None:
                metadata_payload = load_json_object(metadata_path, {})
                if isinstance(metadata_payload, dict) and metadata_payload:
                    rows = _merge_lean_registry_rows(rows, metadata_payload)
            return rows
    seed_path = registry_seed_path_for(path)
    if seed_path is not None and seed_path.exists():
        rows = _load_json_array_from_file(seed_path, fallback)
        metadata_path = _registry_metadata_path_for(path)
        if metadata_path is not None:
            metadata_payload = load_json_object(metadata_path, {})
            if isinstance(metadata_payload, dict) and metadata_payload:
                rows = _merge_lean_registry_rows(rows, metadata_payload)
        return rows
    return [dict(row) for row in fallback]


def load_json_object(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback = dict(default or {})
    try:
        candidates = _json_storage_candidates(Path(path))
        existing = next((candidate for candidate in candidates if candidate.exists()), None)
        if existing is None:
            return fallback
        if existing.suffix == ".gz":
            with gzip.open(existing, mode="rt", encoding="utf-8") as handle:
                payload = json.load(handle)
        else:
            payload = json.loads(existing.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else fallback
    except (OSError, json.JSONDecodeError):
        return fallback


def _write_json_payload_atomic(path: Path, payload: Any) -> None:
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
        last_error: Exception | None = None
        for attempt in range(18):
            try:
                os.replace(tmp, target)
                last_error = None
                break
            except PermissionError as exc:
                last_error = exc
                # Windows can transiently lock the destination while another thread replaces it.
                time.sleep(0.012 * (attempt + 1))
        if last_error is not None:
            raise last_error
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _json_payload_matches_existing(path: Path, payload: Any) -> bool:
    target = _gzip_path_for(path) if _uses_gzip_storage(path) else path
    if not target.exists():
        return False
    if (
        _is_lean_registry_entrypoint(path)
        and isinstance(payload, list)
        and all(isinstance(row, dict) for row in payload)
    ):
        return load_json_array(path, []) == [dict(row) for row in payload]
    return _load_json_payload_from_file(target) == payload


def save_json_atomic(path: Path, payload: Any) -> None:
    path = Path(path)
    if _json_payload_matches_existing(path, payload):
        return
    if (
        _is_lean_registry_entrypoint(path)
        and isinstance(payload, list)
        and all(isinstance(row, dict) for row in payload)
    ):
        core_rows, metadata_map = _prepare_lean_registry_rows_for_write(path, payload)
        _write_json_payload_atomic(path, core_rows)
        metadata_path = _registry_metadata_path_for(path)
        if metadata_path is not None:
            _write_json_payload_atomic(metadata_path, metadata_map)
        return
    _write_json_payload_atomic(path, payload)
