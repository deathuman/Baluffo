"""Default source registries and registry file helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Sequence

from src.jobs.common.config import SOURCE_REGISTRY_ACTIVE_PATH

# NOTE: The registry lists are large; they remain defined in `src.jobs.common` for now
# and are imported here at runtime to avoid circularity during migration.


def load_registry_from_file(path: Path, fallback: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        if not path.exists():
            return [dict(row) for row in fallback]
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return [dict(row) for row in fallback]
        rows = [row for row in payload if isinstance(row, dict)]
        return rows if rows else [dict(row) for row in fallback]
    except (OSError, json.JSONDecodeError):
        return [dict(row) for row in fallback]


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


def load_studio_source_registry(default_registry: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return load_registry_from_file(SOURCE_REGISTRY_ACTIVE_PATH, default_registry)

