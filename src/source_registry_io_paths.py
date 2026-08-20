"""Source registry paths, lean row shaping, and write-retry foundations.

AI boundary owns: DATA_DIR-derived path constants, storage-format classification (gzip /
journal / lean registry), lean-registry row shaping, and the atomic-write retry engine.
AI boundary implement in: this base leaf for storage foundations; loaders live in
``source_registry_io_load.py`` and the journal subsystem in ``source_registry_io_journal.py``.
AI boundary seam: DATA_DIR is rebound at runtime by ``source_registry._sync_io_paths`` — this
leaf resolves it through the coordinator at call time.
"""

from __future__ import annotations

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
_JSON_JOURNAL_DELTA_SCHEMA_VERSION = 2
_JSON_JOURNAL_COMPACT_MAX_BYTES = 1_048_576
_JSON_JOURNAL_HARD_MAX_BYTES = _JSON_JOURNAL_COMPACT_MAX_BYTES * 4
_WRITE_POLICY_REQUIRED = "required"
_WRITE_POLICY_BEST_EFFORT = "best_effort"
_WRITE_RETRY_ATTEMPTS = 18
_WRITE_RETRY_BACKOFF_BASE_S = 0.012

# Files that must never use load_json_array; they are runtime evidence artifacts
# that should be read via load_runtime_evidence or load_runtime_evidence_array.
_RUNTIME_EVIDENCE_FILE_NAMES = {
    "admin-active-task-snapshot.json",
    "jobs-fetch-report.json",
    "jobs-fetch-tasks.json",
    "source-discovery-candidates.json",
    "source-discovery-report.json",
    "sync-live-task.json",
}

_REGISTRY_JOURNAL_FILE_NAMES = {
    "source-registry-active.json",
    "source-registry-pending.json",
    "source-registry-rejected.json",
    "source-registry-tombstones.json",
}

_RUNTIME_EVIDENCE_JOURNAL_QUARANTINE_DIR = "runtime-evidence-journal-quarantine"


def ensure_data_dir() -> None:
    from src import source_registry_io as _srio

    _srio.DATA_DIR.mkdir(parents=True, exist_ok=True)


def _storage_base_name(path: Path) -> str:
    return Path(path).name.removesuffix(".gz")


def _is_runtime_evidence_file(path: Path) -> bool:
    return _storage_base_name(Path(path)) in _RUNTIME_EVIDENCE_FILE_NAMES


def _uses_json_journal(path: Path) -> bool:
    return _storage_base_name(Path(path)) in _REGISTRY_JOURNAL_FILE_NAMES


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
    from src import source_registry_io as _srio

    return _srio.load_json_array(path, default)


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


def _sleep_after_write_failure(attempt: int) -> None:
    from src import source_registry_io as _srio

    time.sleep(_srio._WRITE_RETRY_BACKOFF_BASE_S * (attempt + 1))


def _finish_write_failure(error: OSError, policy: str) -> bool:
    if policy == _WRITE_POLICY_BEST_EFFORT:
        return False
    raise error


def _run_write_with_retries(operation, *, policy: str = _WRITE_POLICY_REQUIRED) -> bool:
    from src import source_registry_io as _srio

    last_error: OSError | None = None
    for attempt in range(_srio._WRITE_RETRY_ATTEMPTS):
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
    allow_target_unlink: bool = True,
) -> bool:
    last_error: OSError | None = None
    for attempt in range(_WRITE_RETRY_ATTEMPTS):
        try:
            os.replace(tmp, target)
            return True
        except OSError as exc:
            last_error = exc
            _sleep_after_write_failure(attempt)
    if (
        last_error is not None
        and policy == _WRITE_POLICY_REQUIRED
        and target.exists()
        and allow_target_unlink
    ):
        try:
            target.unlink()
            os.replace(tmp, target)
            return True
        except OSError as exc:
            last_error = exc
    if last_error is None:
        return True
    return _finish_write_failure(last_error, policy)


def _storage_metrics_data_dir_for(path: Path) -> Path:
    from src import source_registry_io as _srio

    candidate = Path(path).expanduser().resolve()
    data_dir = Path(_srio.DATA_DIR).expanduser().resolve()
    try:
        candidate.relative_to(data_dir)
        return data_dir
    except ValueError:
        return candidate.parent
