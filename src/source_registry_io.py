"""JSON IO and path defaults for source registry files."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.baluffo_config import get_storage_defaults
from src.storage_metrics import duration_ms, record_json_write, record_jsonl_write

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
    DATA_DIR.mkdir(parents=True, exist_ok=True)


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


def load_json_object(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
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
    candidate = Path(path).expanduser().resolve()
    data_dir = Path(DATA_DIR).expanduser().resolve()
    try:
        candidate.relative_to(data_dir)
        return data_dir
    except ValueError:
        return candidate.parent


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
        replaced = _replace_path_with_retry(tmp, target, policy=policy)
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

    root = Path(data_dir) if data_dir is not None else DATA_DIR
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
    latest_payload = _load_json_journal_latest_payload(path)
    if _registry_journal_expects_array(path):
        if isinstance(latest_payload, list):
            return [dict(row) for row in latest_payload if isinstance(row, dict)]
        return load_json_array(path, [])
    if isinstance(latest_payload, dict):
        return dict(latest_payload)
    return load_json_object(path, {})


def compact_registry_journals(data_dir: Path | None = None) -> dict[str, Any]:
    root = Path(data_dir or DATA_DIR)
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
            if byte_size <= _JSON_JOURNAL_COMPACT_MAX_BYTES:
                result["skipped"].append(
                    {
                        "path": str(journal_path),
                        "reason": "below_threshold",
                        "byteSize": byte_size,
                    }
                )
                continue
            payload = _registry_journal_repair_payload(path)
            _write_text_atomic(
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
    if _registry_journal_expects_array(path):
        rows = _load_json_array_from_storage(path, [])
        return rows if rows is not None else []
    return _load_json_object_from_storage(path, {})


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
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_data_dir()
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{time.time_ns()}.tmp")
    try:
        write_started_at = time.perf_counter()
        tmp.write_text(text, encoding="utf-8")
        replaced = _replace_path_with_retry(
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
    if journal_bytes + record_bytes > _JSON_JOURNAL_HARD_MAX_BYTES:
        _write_text_atomic(
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
    if not _uses_json_journal(path):
        return
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
        _registry_journal_record_text(path, payload),
        policy=_WRITE_POLICY_REQUIRED,
    )


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
        row_id = row.get("id")
        if not isinstance(row_id, str) or not row_id:
            return None
        rows_by_id[row_id] = dict(row)
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
    try:
        return int(record.get("rowCount"))
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


def _json_payload_matches_existing(path: Path, payload: Any) -> bool:
    if not _uses_json_journal(path):
        return _canonical_json_payload_matches_existing(path, payload)
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


def _canonical_json_payload_matches_existing(path: Path, payload: Any) -> bool:
    existing = next(
        (candidate for candidate in _json_storage_candidates(path) if candidate.exists()),
        None,
    )
    if existing is None:
        return False
    try:
        return _load_json_payload_from_file(existing) == _json_journal_image_payload(payload)
    except (OSError, json.JSONDecodeError):
        return False


def save_json_atomic(path: Path, payload: Any) -> None:
    path = Path(path)
    if _is_runtime_evidence_file(path):
        if _canonical_json_payload_matches_existing(path, payload):
            return
        _write_json_payload_atomic(path, _json_journal_image_payload(payload))
        return
    if not _uses_json_journal(path):
        if _canonical_json_payload_matches_existing(path, payload):
            return
        _write_json_payload_atomic(path, _json_journal_image_payload(payload))
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
