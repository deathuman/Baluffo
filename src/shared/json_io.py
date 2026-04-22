from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def read_json(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback


def read_json_object(
    path: Path,
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = read_json(path, dict(fallback or {}))
    return payload if isinstance(payload, dict) else dict(fallback or {})
