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

_GZIP_REGISTRY_NAMES = {
    "source-registry-active.json",
    "source-registry-pending.json",
    "source-registry-rejected.json",
    "source-registry-tombstones.json",
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


def load_json_array(
    path: Path, default: list[dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    fallback = default or []
    path = Path(path)
    for candidate in _json_storage_candidates(path):
        if candidate.exists():
            return _load_json_array_from_file(candidate, fallback)
    seed_path = registry_seed_path_for(path)
    if seed_path is not None and seed_path.exists():
        return _load_json_array_from_file(seed_path, fallback)
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


def save_json_atomic(path: Path, payload: Any) -> None:
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
