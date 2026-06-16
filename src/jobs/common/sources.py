"""Default source registries and registry file helpers."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from src import source_registry_io as _registry_io
from src.jobs.common.config import SOURCE_REGISTRY_ACTIVE_PATH
from src.storage import BaluffoStore, BaluffoStoreError
from src.storage.source_registry_runtime import SourceRegistryRuntimeStore

_REGISTRY_SEED_NAMES = {
    "source-registry-active.json": "source-registry-active.seed.json",
    "source-registry-pending.json": "source-registry-pending.seed.json",
}

# NOTE: The registry lists remain defined in their owning leaf modules; do not
# recreate root-package exports in `src.jobs.common`.
# and are imported here at runtime to avoid circularity during migration.


def _looks_like_placeholder_registry_row(row: dict[str, Any]) -> bool:
    name = str(row.get("name") or "").strip().lower()
    studio = str(row.get("studio") or "").strip().lower()
    slug = str(row.get("slug") or "").strip().lower()
    careers_url = str(row.get("careersUrl") or "").strip().lower()
    api_url = str(row.get("api_url") or "").strip().lower()
    return bool(
        slug == "examplestudio"
        or "example studio gmbh (greenhouse)" == name
        or ("example studio gmbh" == studio and "boards.greenhouse.io/examplestudio" in careers_url)
        or "boards-api.greenhouse.io/v1/boards/examplestudio/jobs" in api_url
    )


def registry_seed_path_for(path: Path) -> Path | None:
    return _registry_io.registry_seed_path_for(path)


def load_registry_from_file(path: Path, fallback: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return _registry_io.load_json_array(Path(path), [dict(row) for row in fallback])


def _load_registry_from_sqlite_authority(path: Path) -> list[dict[str, Any]]:
    data_dir = Path(path).expanduser().resolve().parent
    try:
        with BaluffoStore(data_dir) as store:
            if store.get_authority_modes().get("sourceRegistry") != "sqlite":
                return []
            runtime = SourceRegistryRuntimeStore(store)
            state = runtime.current_state()
    except (BaluffoStoreError, OSError, RuntimeError, sqlite3.Error, ValueError):
        return []
    active_rows = state.get("active") if isinstance(state, dict) else []
    return [dict(row) for row in active_rows or [] if isinstance(row, dict)]


def read_approved_since_last_run(path: Path) -> int:
    try:
        if not path.exists():
            return 0
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return 0
        return int(payload.get("approvedSinceLastRun") or 0)
    except (OSError, ValueError, json.JSONDecodeError):
        return 0


def load_studio_source_registry(default_registry: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = _load_registry_from_sqlite_authority(SOURCE_REGISTRY_ACTIVE_PATH)
    if not rows:
        rows = load_registry_from_file(SOURCE_REGISTRY_ACTIVE_PATH, default_registry)
    filtered = [
        row
        for row in rows
        if isinstance(row, dict) and not _looks_like_placeholder_registry_row(row)
    ]
    return filtered if filtered else [dict(row) for row in default_registry]
